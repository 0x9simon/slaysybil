# coding: utf-8
import json, datetime

ntag = "0929"

token_decimal = {
    "ETH": 10 ** 18,
    "WETH": 10 ** 18,
    "DAI": 10 ** 18,
    "USDC": 10 ** 6,
    "USDT": 10 ** 6,
}

def load_price_and_contract(chain_name):
    _p = {}
    with open("token_price.log") as fin:
        a = fin.read().split("\n")
        for x in a:
            if x:
                t = json.loads(x)
                _p[(t["day"], t["token"])] = str(t["avg_price"])

    _c = set()

    _t = None 
    with open("contract_%s_tag.dat" % chain_name) as fin:
        _t = json.loads(fin.read())

    
    _s = set()

    _b = {}
    with open("./%s_blocks_number_timestamp.csv" % chain_name) as fin:
        bf = 100 * 1024 * 1024
        a = fin.readlines(bf)
        while a:
            for linestr in a:
                x = linestr.replace("\n", "")
                if x:
                    items= x.split("|")
                    if items[0] and items[1]:
                        _b[str(int(items[0], 16))] = str(int(items[1], 16))
            a = fin.readlines(bf)

    return _p, _c, _t, _s, _b


def init_chainname(chain_name):
    global price, contract, addr_tag, safe_addr, blocksnum_timestamp
    price, contract, addr_tag, safe_addr, blocksnum_timestamp = load_price_and_contract(chain_name)

init_chainname("polygon")

transfer_headers = ("txn_from", "txn_to", "txn_hash","blocknumber","timestamp","token_txn_from","token_txn_to","gas","gasused","gasprice","token","value","trace_id","log_index","functionname","methodid","nonce", "transactionindex","txreceipt_status", "iserror")
owner_change_header = ("txn_hash","blocknumber","timestamp","txn_from","txn_to","gas","gasused","gasprice","functionname","methodid","nonce","transactionindex","txreceipt_status","safe_action","owner")
        

def format_one(line_value, direction):

    # refrom value
    if line_value["value"].startswith("0x"):
        line_value["value"] = str(int(line_value["value"], 16))

    if line_value["timestamp"]:
        # reform timestamp, add dt
        line_value["dt"] = str(datetime.datetime.fromtimestamp(int(line_value["timestamp"])))
        price_key = (line_value["dt"][:10], line_value["token"]) 
        if price_key in price:
            # link price
            line_value["token_price"] = price[price_key]

            # caculate value, save amount
            line_value["token_amount"] = int(line_value["value"]) * float(line_value["token_price"]) / token_decimal[line_value["token"]]
            line_value["token_amount"] = str(int(line_value["token_amount"]*100)/100)
        else:
            line_value["token_price"] = ""
            line_value["token_amount"] = ""
    else:
        line_value["dt"] = ""
        line_value["token_price"] = ""
        line_value["token_amount"] = ""

    line_value["token_count"] = str(int(line_value["value"])  / token_decimal[line_value["token"]])
    del line_value["value"]
    # refrom value
    if line_value["log_index"].startswith("0x"):
        if line_value["log_index"] == "0x":
            line_value["log_index"] = "0"
        else:
            line_value["log_index"] = str(int(line_value["log_index"], 16))

    if line_value["transactionindex"].startswith("0x"):
        if line_value["transactionindex"] == "0x":
            line_value["transactionindex"] = "0"
        else:
            line_value["transactionindex"] = str(int(line_value["transactionindex"], 16))

    if line_value["nonce"].startswith("0x"):
        if line_value["nonce"] == "0x":
            line_value["nonce"] = "0"
        else:
            line_value["nonce"] = str(int(line_value["nonce"], 16))

    if line_value["blocknumber"].startswith("0x"):
        line_value["blocknumber"] = str(int(line_value["blocknumber"], 16))

    # check from_addr  if token_txn_from is a contract
    if line_value["token_txn_from"] in contract or line_value["token_txn_from"] in addr_tag:
        line_value["from_is_contract"] = 1
    else:
        line_value["from_is_contract"] = 0

    # check if is a safe addr
    if line_value["token_txn_from"] in safe_addr:
        line_value["from_is_contract"] = 2

    line_value["token_from_name"], line_value["token_from_namespace"] = addr_tag.get(line_value["token_txn_from"], ("", ""))

    # check to_addr    if token_txn_to is a contract
    if line_value["token_txn_to"] in contract or line_value["token_txn_to"] in addr_tag:
        line_value["to_is_contract"] = 1
    else:
        line_value["to_is_contract"] = 0

    # check if is a safe addr
    if line_value["token_txn_to"] in safe_addr:
        line_value["to_is_contract"] = 2

    line_value["token_to_name"], line_value["token_to_namespace"] = addr_tag.get(line_value["token_txn_to"], ("", ""))
    
    _map = {
        "in": "to",
        "out": "from"
    }
    # line_value["{direct}_is_contract".format(direct=_map[direction])] = 1

    return line_value


def trans_tranfer(tag, direction):
    
    with open("{tag}_{direct}_{ntag}.csv".format(ntag=ntag, tag=tag, direct=direction)) as fin, \
         open("safe_multisig_{tag}_{direct}_{ntag}_reform.log".format(ntag=ntag, tag=tag, direct=direction), "w") as fout:
        bf = 100*1024*1024
        a = fin.readlines(bf)
        while a:
            for x in set(a):
                if not x:
                    continue
                items = x.replace("\n", "").split("|")
                if len(items) != len(transfer_headers):
                    print(x)
                    break
                else:
                    nd = {transfer_headers[i]: items[i] for i in range(len(transfer_headers))}
                    fout.write(json.dumps(format_one(nd, direction)))
                    fout.write("\n")
            a = fin.readlines(bf)


def trans_owner_change():
    with open("owner_change_{ntag}.csv".format(ntag=ntag)) as fin, open("refine_safe_multisig_owner_change_{ntag}.log".format(ntag=ntag), "w") as fout:  
        a = fin.read().split("\n")  
        for x in set(a):
            if not x:
                continue
            items = x.split("|")
            if len(items) != len(owner_change_header):
                print(x)
                break
            else:
                nd = {owner_change_header[i]: items[i] for i in range(len(owner_change_header))}
                if nd["timestamp"].startswith("0x"):
                    nd["timestamp"] = str(int(nd["timestamp"], 16))
                    nd["nonce"] = str(int(nd["nonce"], 16))
                    nd["blocknumber"] = str(int(nd["blocknumber"], 16))
                    if nd["gas"]:
                        nd["gas"] = str(int(nd["gas"], 16))
                    if nd["gasprice"]:
                        nd["gasprice"] = str(int(nd["gasprice"], 16))
                    if nd["gasused"]:
                        nd["gasused"] = str(int(nd["gasused"], 16))
                
                fout.write(json.dumps(nd))
                fout.write("\n")


def trans_owner_info():
    with open("./safe_owner_info_{ntag}.csv".format(ntag=ntag)) as fin, open("refine_safe_multisig_info_{ntag}.log".format(ntag=ntag), "w") as fout:
     a = fin.read().split("\n")
     b = ("contract_creator","blocknumber","nonce","timestamp","txreceipt_status","contract_factory","contract_address","create_txn")
     for x in set(a):
         if not x:
             continue
         items = x.split("|")
         if len(items) != len(b):
             print(repr(x))
             break
         else:
             nd = {b[i]: items[i] for i in range(len(b))}
             if nd["timestamp"].startswith("0x"):
                 nd["timestamp"] = str(int(nd["timestamp"], 16))
                 nd["nonce"] = str(int(nd["nonce"], 16))
                 nd["blocknumber"] = str(int(nd["blocknumber"], 16))
 
             nd["dt"] = str(datetime.datetime.fromtimestamp(int(nd["timestamp"])))
             fout.write(json.dumps(nd))
             fout.write("\n")

def deal_transafer_raw(fn):
    with open("transafer_raw/%s" % fn) as fin, open("transafer_raw/trans_%s" % fn, "w") as fout:
        bf = 100 * 1024 * 1024
        a = fin.readlines(bf)
        while a:
            for linedata in a:
                if linedata:
                    _lines = []
                    bpos = linedata.find("}{")
                    if bpos > 0:
                        _lines.append(linedata[:bpos+1])
                        _lines.append(linedata[bpos+1:])
                    else:
                        _lines.append(linedata)

                    
                    for line_str in _lines:
                        
                        t = json.loads(line_str)
                        t["blockNum"] = str(int(t["blockNum"], 16))
                        if t["blockNum"] not in blocksnum_timestamp:
                            continue
                        t["timestamp"] = blocksnum_timestamp[t["blockNum"]]

                        try:
                            t["rawContract_value"] = str(int(t["rawContract"]["value"], 16))
                            t["rawContract_address"] = t["rawContract"]["address"]
                            t["rawContract_decimal"] = str(int(t["rawContract"]["decimal"], 16))
                        except Exception as e:
                            t["rawContract_value"] = t["rawContract"]["value"]
                            t["rawContract_address"] = t["rawContract"]["address"]
                            t["rawContract_decimal"] = t["rawContract"]["decimal"]

                        t["erc1155Metadata"] = json.dumps(t["erc1155Metadata"])

                        t["chain_name"] = "ethereum"
                        t["token"] = t["asset"]
                        t["dt"] = str(datetime.datetime.fromtimestamp(int(t["timestamp"])))
                        t["token_txn_from"] = t["from"]
                        t["token_txn_to"] = t["to"]
                        if t["token_txn_from"] in contract or t["token_txn_from"] in addr_tag:
                            t["txn_from_type"] = 1
                        else:
                            t["txn_from_type"] = 0

                        if t["token_txn_to"] in contract or t["token_txn_to"] in addr_tag:
                            t["txn_to_type"] = 1
                        else:
                            t["txn_to_type"] = 0

                        # check if is a safe addr
                        if t["token_txn_to"] in safe_addr:
                            t["txn_to_type"] = 2

                        # check if is a safe addr
                        if t["token_txn_from"] in safe_addr:
                            t["txn_from_type"] = 2

                        t["token_to_name"], t["token_to_namespace"] = addr_tag.get(t["token_txn_to"], ("", ""))

                        t["token_count"] = str(t["value"])
                        # link price
                        if t["token"] in ("ETH", "WETH", "USDC", "USDT", "DAI") and (t["dt"][:10], t["token"]) in price:
                            t["token_price"] = price[t["dt"][:10], t["token"]]
                            # caculate value, save amount
                            if t["value"] and t["token_price"]:
                                t["token_amount"] = float(t["value"]) * float(t["token_price"])
                                t["token_amount"] = str(int(t["token_amount"]*100)/100)
                            else:
                                t["token_price"] = ""
                                t["token_amount"] = ""  
                        else:
                            t["token_price"] = ""
                            t["token_amount"] = ""       

                        del t["rawContract"]
                        del t["value"]

                        fout.write(json.dumps(t))
                        fout.write("\n")
            a = fin.readlines(bf)

def deal_zksync_transfer_raw(fn):
    all_keys = set()
    with open("transafer_raw/%s" % fn) as fin, open("transafer_raw/trans_%s" % fn, "w") as fout:
        bf = 100 * 1024 * 1024
        a = fin.readlines(bf)
        while a:
            for linedata in a:
                if linedata:
                    _lines = []
                    bpos = linedata.find("}{")
                    if bpos > 0:
                        _lines.append(linedata[:bpos+1])
                        _lines.append(linedata[bpos+1:])
                    else:
                        _lines.append(linedata)

                    for line_str in _lines:
                        
                        t = json.loads(line_str)
                        tx_id = t["tx_id"]
                        t["blockNum"], t["txnIndex"] = tx_id.split(",")
                        
                        try:
                            t["timestamp"] = str(int(datetime.datetime.strptime(t["created_at"], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()))
                            t["dt"] = str(datetime.datetime.fromtimestamp(int(t["timestamp"])))
                        except:
                            t["timestamp"] = str(int(datetime.datetime.strptime(t["created_at"], '%Y-%m-%dT%H:%M:%SZ').timestamp()))
                            t["dt"] = str(datetime.datetime.fromtimestamp(int(t["timestamp"])))

                        try:
                            _t = json.loads(t["tx_signature"])
                            for ks in _t:
                                t["tx_signature_%s" % ks] = _t[ks]
                            del t["tx_signature"]

                        except Exception as e:
                            pass

                        
                        t["chain_name"] = "zksync"

                        if "tx_from" in t and "tx_to" in t:
                            t["token_txn_from"] = t["tx_from"]
                            t["token_txn_to"] = t["tx_to"]
                            if t["token_txn_from"] in contract or t["token_txn_from"] in addr_tag:
                                t["txn_from_type"] = 1
                            else:
                                t["txn_from_type"] = 0

                            if t["token_txn_to"] in contract or t["token_txn_to"] in addr_tag:
                                t["txn_to_type"] = 1
                            else:
                                t["txn_to_type"] = 0

                            # check if is a safe addr
                            if t["token_txn_to"] in safe_addr:
                                t["txn_to_type"] = 2

                            # check if is a safe addr
                            if t["token_txn_from"] in safe_addr:
                                t["txn_from_type"] = 2

                            t["token_to_name"], t["token_to_namespace"] = addr_tag.get(t["token_txn_to"], ("", ""))

                            if "tx_token" in t and "tx_amount" in t:
                                t["token"] = t["tx_token"]
                                t["token_count"] = str(t["tx_amount"])
                                # link price
                                if t["token"] in ("ETH", "WETH", "USDC", "USDT", "DAI") and (t["dt"][:10], t["token"]) in price:
                                    t["token_price"] = price[t["dt"][:10], t["token"]]
                                    t["token_count"] = str(int(t["tx_amount"])  / token_decimal[t["token"]])
                                    # caculate value, save amount
                                    if t["tx_amount"] and t["token_price"]:
                                        t["token_amount"] = float(t["tx_amount"]) * float(t["token_price"])
                                        t["token_amount"] = str(int(t["token_amount"]*100)/100)
                                    else:
                                        t["token_price"] = ""
                                        t["token_amount"] = ""  
                                else:
                                    t["token_price"] = ""
                                    t["token_amount"] = ""       

                            
                            del t["tx_from"]
                            del t["tx_to"]
                        for ks in t:
                            t[ks] = str(t[ks])
                            if ks not in all_keys:
                                all_keys.add(ks)
                        fout.write(json.dumps(t))
                        fout.write("\n")
            a = fin.readlines(bf)
    print("\t".join(list(all_keys)))

def output_csv(fn):
                
    with open(fn) as fin, open("csv_%s" % fn, "w") as fout:
        a = fin.read().split("\n")
        import json
        headers = ('blockNum', 'uniqueId', 'hash', 'from', 'to', 'erc721TokenId', 'erc1155Metadata', 'tokenId', 'asset', 'category', 'timestamp', 'rawContract_value', 'rawContract_address', 'rawContract_decimal', 'chain_name', 'token', 'dt', 'token_txn_from', 'token_txn_to', 'txn_from_type', 'txn_to_type', 'token_to_name', 'token_to_namespace', 'token_count', 'token_price', 'token_amount')
        for x in a:
            if x:
                d = json.loads(x)
                items = [str(d[t]) for t in headers]
                fout.write("\t".join(items))
                fout.write("\n")



def trans_fdd():
    
    trans_tranfer("eth", "in")
    trans_tranfer("eth", "out")
    trans_tranfer("eth_internal", "in")
    trans_tranfer("token", "in")
    trans_tranfer("token", "out")
