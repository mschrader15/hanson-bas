import urllib3

"""
Max moved these to a seperate module because they aren't used

"""


def _inner_fetch(device, num, process_func):
    """
    The inner function for the multithreaded fetch
    
    :param device: item of the Device class
    :param num: the number of the thread
    :return: 
    """
    http = urllib3.PoolManager()
    try:
        print("GET request to ", device.ip_address)
        r = http.request('GET', device.ip_address, timeout=urllib3.Timeout(connect=2.0))
        device = process_func(device, xml_as_obj=r.data.decode("utf-8"))
        # except urllib3.exceptions.MaxRetryError:
    except Exception as e:
        print(device.name, e)
    return num, device


def fetch_data_multi_threaded(master_dict, process_func):
    from concurrent.futures import ThreadPoolExecutor, as_completed	
    # See:  https://creativedata.stream/multi-threading-api-requests-in-python/		
    # it is possible that I will need to wrap this in a timeout function due threading hang-up on windows...
    threads = []
    with ThreadPoolExecutor(max_workers=len(master_dict.values())) as executor:
        for num, device in enumerate(master_dict.values()):
            threads.append(executor.submit(_inner_fetch, device, num, process_func))
        results = sorted([res for res in [task.result() for task in as_completed(threads)]], key=lambda x: x[0])
        for num, device in enumerate(master_dict.values()):
            device = results[num][1]
    return master_dict


def func_wrapper(func, args, static_args, num):
    result = func(args, static_args)
    return num, result


def execute_function_multi_threads(func, split_args, static_args, threadnum):
    import numpy as np
    from concurrent.futures import ThreadPoolExecutor, as_completed

    for i, arg in enumerate(split_args):
        # for j, arg_in in enumerate(arg):
        split_args[i] = np.array_split(arg, threadnum)
    threads = []
    with ThreadPoolExecutor(max_workers=threadnum) as executor:
        for num, args in enumerate(list(zip(*split_args))):
            threads.append(executor.submit(func_wrapper, func, args, static_args, num))
        results = sorted([res for res in [task.result() for task in as_completed(threads)]], key=lambda x: x[0])
        # for num, device in enumerate(master_dict.values()):
        #     device = results[num][1]
    return results
