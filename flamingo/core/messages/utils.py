import socket
import io
import pickle
from . import params
from .message import Message
import psutil
import os
from functools import partial
import logging
import signal
import random
import time
from multiprocessing import Process

def add_log(my_node, log, ty):
    # print(log)
    my_node.log_q.put((ty, log))
    os.kill(my_node.pids['logging'], signal.SIGUSR1)

def send_heartbeat(my_node, to):
    cur_res = get_resources()
    my_node.resources[my_node.self_ip] = cur_res

    jobQ_cp = []
    for job_i in my_node.jobQ:
        jobQ_cp.append(job_i)

    msg = Message('HEARTBEAT', content = [jobQ_cp, cur_res])
    
    my_node.last_jobs_sent = len(msg.content[0])
    send_msg(msg, to, my_node = my_node)

def sleep_and_ping_backup(my_node, to):
    time.sleep(params.BACKUP_HEARTBEAT_INTERVAL)
    msg = Message('BACKUP_HEARTBEAT')
    send_msg(msg, to, my_node = my_node)

def sleep_and_ping(my_node, to):
    time.sleep(params.HEARTBEAT_INTERVAL)
    msg = Message('ARE_YOU_ALIVE')
    send_msg(msg, to, my_node = my_node)

def start_job(my_node, job_id, recv_ip):
    add_log(my_node, "Starting job " + job_id, "INFO")
    cmd = "./executable < input > " +  "../../" + params.LOG_DIR + "/" + job_id 
    
    exec_p = Process(target = exec_new_job, args = (my_node, job_id, cmd, recv_ip))
    exec_p.start()
    my_node.job_pid[job_id] = exec_p.pid
    
def exec_new_job(my_node, job_id, cmd, source_ip):
    os.chdir(os.path.join(params.EXEC_DIR, job_id))
    st_tm = time.time()
    os.system(cmd)
    end_tm = time.time()

    job_run_time = end_tm - st_tm
    tat = end_tm - my_node.job_submitted_time[job_id]

    add_log(my_node, "Completed job " + job_id, "INFO")
    msg = Message('COMPLETED_JOB', content = [job_id, job_run_time, tat])
    send_msg(msg, to = my_node.ip_dict['root'], my_node = my_node)

    del my_node.job_pid[job_id]
    os.system("rm -rf " + os.path.join(params.EXEC_DIR, job_id))
    
    # After running got completed remove this job from individual_running_jobs
    del my_node.individual_running_jobs[job_id]


    log_ip = source_ip
    if source_ip == my_node.self_ip:
        msg = Message('GET_ALIVE_NODE', content = [source_ip, job_id])
        send_msg(msg, to = my_node.ip_dict['root'], my_node = my_node)
    else:
        send_file("../../" + os.path.join(params.LOG_DIR, job_id), to = log_ip, job_id = job_id, file_ty = "log", my_node = my_node)    

    # send leader msg to remove this job from running Q

def get_random_alive_node(resources, not_ips = None):
    while 1:
        ip = random.choice(list(resources.keys()))
        if not ip in not_ips:
            return ip

def get_job_status(my_node, jobid): # expects my_node to be leader
    reply = "Waiting"
    if jobid in my_node.completed_jobs.keys():
        reply = "Completed"
    
    # print(my_node.running_jobs)
    for key in my_node.running_jobs.keys():
        for job in my_node.running_jobs[key]:
            if jobid == job.job_id:
                reply = "Running"
                break

    return reply

def create_logger(log_level = logging.INFO, log_filename = None):
    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if log_filename:
        fh = logging.FileHandler(log_filename)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # ch = logging.StreamHandler()
    # ch.setLevel(log_level)
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)

    return logger

def get_resources():
    res = {
        'memory' : psutil.virtual_memory().available >> 20,
        'cpu_usage' : psutil.cpu_percent(),
        'cores' : psutil.cpu_count(),
        'process_load' : os.getloadavg()[1]
    }
    return res

def create_socket(to , my_node):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if not my_node.le_elected:
        while 1:
            try:
                sock.connect((to, params.CLIENT_RECV_PORT))
                break
            except ConnectionRefusedError:
                pass
    else:
        flg = 0 
        for i in range(params.MAX_SEND_RETRIES):
            try:
                sock.connect((to, params.CLIENT_RECV_PORT))
                flg = 1
                break
            except ConnectionRefusedError:
                pass

        if flg == 0 :
            return None

    return sock

# check if alive, return true/false
def send_msg(msg, to, sock = None, my_node = None):
    if not sock:
        sock = create_socket(to, my_node)
        if not sock:
            my_node.failed_msgs.append(msg)
            return

    msg_data = io.BytesIO(pickle.dumps(msg))

    while True:
        chunk = msg_data.read(params.BUFFER_SIZE)

        if not chunk:
            break

        sock.send(chunk)

    sock.shutdown(socket.SHUT_WR)
    
    ty = "INFO"
    # if 'HEARTBEAT' in msg.msg_type:
    #     ty = "DEBUG" 

    add_log(my_node, 'sent msg of type %s to %s' % (msg.msg_type, to), ty)
    # print('sent msg of type %s to %s' % (msg.msg_type, to))
    
def send_file(filepath, to, job_id, file_ty, my_node):
    sock = create_socket(to, my_node)

    if file_ty == 'log':
        msg_ty = 'LOG_FILE'
    elif file_ty == 'fwd_display_output_ack':
        msg_ty = 'FWD_DISPLAY_OUTPUT_ACK'
    else:
        msg_ty = 'FILES_CONTENT'

    msg = Message(msg_ty)
    msg.content = [job_id, file_ty]

    data_list = []
    with open(filepath, 'rb') as fp:
        for chunk in iter(partial(fp.read, 1024), b''):
            data_list.append(chunk)

    data = b''.join(data_list)
    msg.content.append(data)

    send_msg(msg, to, sock, my_node)

def recv_msg(conn):
    data = conn.recv(params.BUFFER_SIZE)
    data_list = []

    while data:
        data_list.append(data)
        data = conn.recv(params.BUFFER_SIZE)

    data = b''.join(data_list)
    msg = pickle.loads(data)
    assert isinstance(msg, Message), "Received object on socket not of type Message."

    return msg

def Managerdict_to_dict(mng_dict):
    tmp_dict = {}
    for i in mng_dict.keys():
        tmp_dict[i] = mng_dict[i]

    return tmp_dict 

def Managerlist_to_list(mng_list):
    tmp_list = []
    for i in mng_list:
        tmp_list.append(i)

    return tmp_list 



def get_leaderstate(my_node):

    tmp_resources = Managerdict_to_dict(my_node.resources)
    tmp_jobQ = Managerlist_to_list(my_node.jobQ)
    tmp_running = Managerdict_to_dict(my_node.running_jobs)
    tmp_leader_joblist = Managerlist_to_list(my_node.leader_joblist)
    state = [my_node.all_ips,my_node.last_jobs_sent,my_node.completed_jobs, \
                tmp_resources,tmp_jobQ,tmp_running,tmp_leader_joblist]

    return state
