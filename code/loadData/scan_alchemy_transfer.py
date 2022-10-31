#-*- coding: utf-8 -*-
import json
import requests
import time
import os
from scan_settings import fetch_settings

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 


class ScanFetcher(object):

    def __init__(self, chain_name, task, batch_id, total_batch, input_file, union_file ):
        self.chain_name = chain_name
        self.task = task
        self.batch_id = batch_id
        self.total_batch = total_batch
        self.input_file = input_file
        self.union_file = union_file

        self.workspace = "scan_data"

        #request settings
        self.rate_limit = 5
        self.retry_times = 5
        #self.total_limit = 100000
        self.perpage = 1000

        # output settings
        self.cnt_per_file = 100000

        self.out_data_fnstr = "{output_path}/{task}_{chain_name}_data_{fileno}_{batchid}.log"
        self.out_abi_fnstr = "{output_path}/{task}_{chain_name}_abi_{fileno}_{batchid}.log"
        self.error_log_fnstr = "{output_path}/{task}_{chain_name}_error_{fileno}_{batchid}.log"

        self.s = None

        self.out_file, self.abi_file, self.error_file = None, None, None



    def init_settings(self, start_num):
        self.key_size = 1
        if self.total_batch <= 0:
            self.total_batch = self.key_size
        self.batch_id = self.batch_id % self.total_batch

        # fetch_settings
        self.fetch_settings = fetch_settings["alchemy_%s" % self.chain_name]
        self.alchemy_transfer_out_url = self.fetch_settings["url_str"].format(apikey=self.fetch_settings["api_key_list"][0])
        self.alchemy_transfer_in_url  = self.fetch_settings["url_str"].format(apikey=self.fetch_settings["api_key_list"][-1])

        self.current_key_list = self.fetch_settings["api_key_list"]
        self.key_size = len(self.current_key_list)
        self.batch_id = self.batch_id % self.key_size
        self.current_key = self.current_key_list[self.batch_id]


        if not os.path.isdir(self.workspace):
            os.mkdir(self.workspace)

        self.output_path = "%s/%s_%s" % (self.workspace, self.task, self.batch_id)
        if not os.path.isdir(self.output_path):
            os.mkdir(self.output_path)

        self.start_num = int(start_num/self.key_size) * self.key_size + self.batch_id

        self.s = requests.Session()


    def format_address(address):
        return address.replace(" ", "").replace("\n", "").replace("\"", "").replace("\\x", "0x")


    def get_alchemy_transfer_in(self, address, pageKey):
        target_url = self.alchemy_transfer_in_url

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params":[
                {
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                    "category": [
                            "external",
                            "internal",
                            "erc721",
                            "erc1155",
                            "erc20"
                    ],
                    "withMetadata": False,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                    "toAddress": address
                }
                ]
        }
        if pageKey:
            payload["params"][0]["pageKey"] = pageKey

        for i in range(self.retry_times):
            if self.request_cnt % self.rate_limit == 0:
                time.sleep(1)

            resp = self.s.post(target_url, json=payload, headers=headers, verify=False, timeout=60)

            self.request_cnt += 1
            if resp.status_code == 200:
                break
            else:
                print(resp.content, flush=True)
                time.sleep(5+i*5)
                self.s = requests.Session()
            
        return resp



    def get_alchemy_transfer_out(self, address, pageKey):
        target_url =self.alchemy_transfer_out_url

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params":[
                {
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                    "category": [
                            "external",
                            "internal",
                            "erc721",
                            "erc1155",
                            "erc20"
                    ],
                    "withMetadata": False,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                    "fromAddress": address
                }
                ]
        }
        if pageKey:
            payload["params"][0]["pageKey"] = pageKey

        for i in range(self.retry_times):
            if self.request_cnt % self.rate_limit == 0:
                time.sleep(1)

            resp = self.s.post(target_url, json=payload, headers=headers, verify=False, timeout=60)

            self.request_cnt += 1
            if resp.status_code == 200:
                break
            else:
                print(resp.content, flush=True)
                time.sleep(5+i*5)
                self.s = requests.Session()
            
        return resp


    def get_output_files(self, fileno):

        if self.out_file is not None:
            self.out_file.close()
        if self.abi_file is not None:
            self.abi_file.close()
        if self.error_file is not None:
            self.error_file.close()
            
        self.out_file = open(self.out_data_fnstr.format(
                    output_path=self.output_path,
                    task=self.task,
                    chain_name = self.chain_name,
                    fileno=fileno,
                    batchid = self.batch_id
                ), "a")
        self.abi_file = open(self.out_abi_fnstr.format(
                    output_path=self.output_path,
                    task=self.task,
                    chain_name = self.chain_name,
                    fileno=fileno,
                    batchid = self.batch_id
                ), "a")
        self.error_file = open(self.error_log_fnstr.format(
                    output_path=self.output_path,
                    task=self.task,
                    chain_name = self.chain_name,
                    fileno=fileno,
                    batchid = self.batch_id
                ), "a")

        return


    def record_error(self, e, resp, address):
        self.error_file.write(repr(e))
        self.error_file.write("\n")
        self.error_file.write(repr(resp.content))
        self.error_file.write("\n") 
        self.error_file.write(str(resp.status_code))
        self.error_file.write("\n") 
        self.error_file.write("failed fetch: %s \n" % (address))
        print("parse error, begin sleep")
        print(resp.content)



    def record_transfer(self, data):
        
        self.out_file.write(json.dumps(data).replace("\n", ""))
        self.out_file.write("\n")


    def loop_transfer_out(self, address):
        
        pageKey = ""
        _cnt = 0
            
        while pageKey is not None and _cnt < 100:

            resp = self.get_alchemy_transfer_out(address, pageKey)
            _cnt += 1
            try:
                content = json.loads(resp.text)
                if "result" in content and isinstance(content["result"], dict) and "transfers" in content["result"]:
                    for tx in content["result"]["transfers"]:
                        self.record_transfer(tx)
                    pageKey = content["result"].get("pageKey", None)
                    print("get pageKey: %s" % pageKey)
                else:
                    raise Exception
            except Exception as e:
                self.record_error(e, resp, address)
                time.sleep(5)
                break

    def loop_transfer_in(self, address):
        
        pageKey = ""
        _cnt = 0
            
        while pageKey is not None and _cnt < 100:

            resp = self.get_alchemy_transfer_in(address, pageKey)
            _cnt += 1
            try:
                content = json.loads(resp.text)
                if "result" in content and isinstance(content["result"], dict) and "transfers" in content["result"]:
                    for tx in content["result"]["transfers"]:
                        self.record_transfer(tx)
                    pageKey = content["result"].get("pageKey", None)
                    print("get pageKey: %s" % pageKey)
                else:
                    raise Exception
            except Exception as e:
                self.record_error(e, resp, address)
                time.sleep(5)
                break


    def run_scan(self):
        self.s = requests.Session()
        self.request_cnt = 0
        
        with open(self.input_file, "r") as fin:
            lines = fin.read().split("\n")

        if self.union_file:
            with open(self.union_file, "r") as fin:
                unions = set(fin.read().split("\n"))
        else:
            unions = set()
            

        total_lines = len(lines)
        account_no = self.start_num 
        current_fileno = int(account_no/self.cnt_per_file) + 1
        self.get_output_files(current_fileno)

        while account_no < total_lines:

            line_str = lines[account_no]
            if line_str:
                items = line_str.split(",") 
                address = ScanFetcher.format_address(items[0])
                if address and address not in unions:
                    if self.loop_contract_sourcecode(address):
                        print("find contract: %s-%s-%s\n" % (self.chain_name, account_no, address))
                    else:
                        print("loop transfer out: %s-%s-%s\n" % (self.chain_name, account_no, address))
                        self.loop_transfer_out(address)
                        print("loop transfer in: %s-%s-%s\n" % (self.chain_name, account_no, address))
                        self.loop_transfer_in(address)
                
            account_no += self.total_batch
            _filenno = int(account_no/self.cnt_per_file) + 1
            if current_fileno != _filenno:
                current_fileno = _filenno
                
                self.get_output_files(current_fileno)
            

if __name__ == "__main__":
    
    import argparse
    scan_argparse = argparse.ArgumentParser()
    scan_argparse.description = "need input: -c chain_name -t task -b batch_id -i input_file contain address to scan -u union file"
    scan_argparse.add_argument("-c", "--chain", help="the chain to scan", type=str, dest="chain_name")
    scan_argparse.add_argument("-t", "--task", help="name the scan task", type=str, dest="task")
    scan_argparse.add_argument("-b", "--batchid", help="you may have multi keys to scan , select one key", type=int, dest="batchid")
    scan_argparse.add_argument("-a", "--totalbatch", help="batch number", type=int, dest="totalbatch", default=-1)
    scan_argparse.add_argument("-i", "--inputfile", help="the file contains the address list to scan", type=str, dest="input_file")
    scan_argparse.add_argument("-s", "--start", help="the start num", dest="start_num", type=int,  default=0)
    scan_argparse.add_argument("-u", "--union", help="the union file", dest="union_file", type=str,  default="")

    args = scan_argparse.parse_args()
    
    scan = ScanFetcher(args.chain_name, args.task, args.batchid, args.totalbatch, args.input_file, args.union_file)
    scan.init_settings(args.start_num)
    scan.run_scan()

 