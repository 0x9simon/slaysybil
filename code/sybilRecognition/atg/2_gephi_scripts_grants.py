import json
import datetime
import networkx as nx
import math


id_spliter = "_"
in_degree_avg = 5
out_degree_avg = 5
size_threshold = 1
interact_threshold = 0.25
react_threshold = 0.64
single_threshold = 0.8

def load_exclude():
    with open("data/exclude_nodes.csv") as fin:
        _a = fin.read().split("\n")
    return set(_a)

def load_address_tags():
    _t = None 
    with open("contract_ethereum_tag.dat") as fin:
        _t = json.loads(fin.read())

    _s = set()
    
    _m = {}
    with open("data/address_tags.csv") as fin:
        a = fin.read().split("\n")
        for x in a:
            if x:
                items = x.split("\t")
                _m[items[0]] = tuple(items[1:])

    _g = {}
    with open("data/gr15_address_tag.csv") as fin:
        a = fin.read().split("\n")
        for x in a:
            if x:
                items = x.lower().split("\t")
                _i = items[0]
                if _i in _g and _g[_i][0] != "contributor":
                    continue
                else:
                    _g[_i] = tuple(items[1:])

    return _t, _s, _m, _g


def load_edges(exclude_nodes=None, fn="data/raw_edges.log", sp="\t", headers=('timestamp','source','target','dt','token_count','chain')):
    edges = []
    edges_ext = {}
    with open(fn) as fin:
        a = fin.read().replace('"', "").split("\n")
        for x in a:
            if x:
                items = x.split(sp)
                if len(items) < len(headers):
                    continue

                _fix = len(headers)
                _edge = dict(zip(headers, items[:_fix]))

                if exclude_nodes is not None and ( _edge["source"] in exclude_nodes or _edge["target"] in exclude_nodes):
                    continue
                else:
                    edges.append(_edge)
                    edges_ext[(_edge["source"], _edge["target"])] = items[_fix:]
    return edges, edges_ext

                
def merge_interval(target_list, interval):
    if not target_list:
        target_list.append(interval)
    else:
        _i = target_list[-1]
        if _i[1] >= interval[0]:
            target_list[-1] = (_i[0], interval[1])
        else:
            target_list.append(interval)


def add_vertex(vertex_set, vertex, d):
    if vertex not in vertex_set:
        vertex_set[vertex] = {
            "count": 1,
            "label": "",
            "abs": d
        }
    else:
        vertex_set[vertex]["count"] += 1
        vertex_set[vertex]["abs"] += d


def add_edge(edge_set, vertex_set, _weights, graph,  edge):

    ts = int(edge["timestamp"])
    dt = str(datetime.datetime.fromtimestamp(ts))
    source = edge["source"]
    target = edge["target"]

    if source == target:
        return 

    chain = edge["chain"]
    try:
        weight = float(edge["token_count"])
    except:
        return

    edge_key = (source, target)

    graph.add_edge(source, target)

    add_vertex(vertex_set, source, 1)
    add_vertex(vertex_set, target, -1)

    if edge_key not in edge_set:
        edge_set[edge_key] = {
            "source": source,
            "target": target,
            "chain": chain,
            "timestamp": edge["timestamp"],
            "dtset":[dt,],
            "timeset":[(ts, ts+10),],
        }
        
    else:
        edge_set[edge_key]["dtset"].append(ts)
        
        merge_interval(edge_set[edge_key]["timeset"], (ts, ts+10))

    add_weight(source, target, _weights, weight)


def add_weight(source, target, _weights, weight):
    
    edge_key = (source, target)
    if edge_key not in _weights:
        _weights[edge_key] = weight
    else:
        _weights[edge_key] += weight

    if ('ALL', target) in _weights:
        _weights[('ALL', target)] += weight
    else:
        _weights[('ALL', target)] = weight

    if (source, 'ALL') in _weights:
        _weights[(source, 'ALL')] += weight
    else:
        _weights[(source, 'ALL')] = weight


    if ('ALL', source) not in _weights:
        _weights[('ALL', source)] = 0.0

    if (target, 'ALL') not in _weights:
        _weights[(target, 'ALL')] = 0.0


def add_gephi_timeset(edge_list):
    edge_set = {}
    _list = edge_list.copy()
    _list.sort(reverse=False, key=lambda x: int(x["timestamp"]))
    vertex_set = {}
    _weights = {}
    _g = nx.DiGraph()

    for edge in _list:
        add_edge(edge_set, vertex_set, _weights, _g, edge)
        
    return edge_set, vertex_set, _weights, _g


def calc_interact_weight(source, target, token_map):
    if token_map[("ALL", target)] < 0.0000001 or token_map[(source, "ALL")] < 0.0000001:
        return 0.0
    else:
        return token_map[(source, target)]/token_map[("ALL", target)] * token_map[(source, target)]/token_map[(source, "ALL")]

def calc_single_weight(source, target, token_map):
    if token_map[("ALL", target)] < 0.0000001 or token_map[(source, "ALL")] < 0.0000001:
        return 0.0
    else:
        return max(token_map[(source, target)]/token_map[("ALL", target)],  token_map[(source, target)]/token_map[(source, "ALL")])


def calc_react_weight(source, target, token_map):
    default = 0.1
    if (source, target) not in token_map or (target, source) not in token_map:
        return default
    elif token_map[(source, target)] < 0.0000001 or token_map[(target, source)] < 0.0000001:
        return default
    elif token_map[("ALL", target)] < 0.0000001 or token_map[(source, "ALL")] < 0.0000001:
        return default
    else:
        return default + token_map[(source, target)]/token_map[("ALL", target)] * token_map[(target, source)]/token_map[(source, target)]


def tag_vertex(vertex_set, count_map, exclude_nodes, graph):
    _ag = graph.copy()
    tags, safe, mantag, gr15 = load_address_tags()
    
    D = _ag.degree()
    iD = _ag.in_degree()
    oD = _ag.out_degree()

    _weights1 = {}
    _weights2 = {}
    _weights3 = {}
    
    #for s, t in G.edges:
    #    weights[(s, t)] =  1/oD(s) * 1/iD(t)
    #_th = 1/(in_degree_avg * out_degree_avg)

    
    for s, t in _ag.edges:
        _weights1[(s, t)] = calc_interact_weight(s, t, count_map)
        _weights2[(s, t)] = calc_react_weight(s, t, count_map)
        _weights3[(s, t)] = calc_single_weight(s, t, count_map)
        

    for s, t in _weights1:

        if _weights1[(s,t)] >= interact_threshold:
            continue
        elif _weights2[(s,t)] >= react_threshold:
            continue
        elif _weights3[(s,t)] >= single_threshold:
            continue
        else:
            _ag.remove_edge(s, t)

    D = _ag.degree()
    iD = _ag.in_degree()
    oD = _ag.out_degree()
        
    removed = set()

    for x in vertex_set:
        
        if x in removed:
            continue

        vertex_set[x]["degree"] = D[x]
        vertex_set[x]["in_degree"] = iD[x]
        vertex_set[x]["out_degree"] = oD[x]
        
        if x in safe:
            vertex_set[x]["label"] = "gnosis_safe"
        elif x in tags:
            vertex_set[x]["label"] = "%s" % "_".join(tags[x])
        elif x in mantag:
            vertex_set[x]["label"] = "%s" % "_".join(mantag[x])
        
        if x in gr15:
            vertex_set[x]["label"] = "%s" % "_".join(gr15[x])
        
        #if x in gr15 and gr15[x][0] in ("grant", "application"):
        #    _ag.remove_node(x)
        #    removed.add(x)

        if x in exclude_nodes:
            _ag.remove_node(x)
            removed.add(x)
        
        elif not x or not x.strip():
            _ag.remove_node(x)
            removed.add(x)

        #elif iD[x] == 0 or oD[x] == 0:
        #    _ag.remove_node(x)
        #    removed.add(x)

        
    _components = [c for c in sorted(nx.strongly_connected_components(_ag), key=len, reverse=True)]
    _sg = []
    _og = []
    for i in range(len(_components)):
        _sg.append(graph.subgraph(_components[i]).copy())
        for x in _components[i]:
            if x in vertex_set:
                vertex_set[x]["comp_id"] = i
                vertex_set[x]["comp_size"] = len(_components[i])
                _og.append((i, len(_components[i])))
        
    return _sg, _og, _weights1, _weights2


def tag_edge_and_vertex(edges, vertexes, sg):
    
    for x in vertexes:
        vertexes[x]["tag"] = str(vertexes[x]["abs"]/vertexes[x]["count"])

    for source, target in edges:
        edges[(source, target)]["tag"] = str(format_tag(vertexes[source]["abs"] - vertexes[target]["abs"]))
    
        edges[(source, target)]["source_comp_id"] = vertexes[source].get("comp_id", None)
        edges[(source, target)]["source_comp_size"] = vertexes[source].get("comp_size", None)

        edges[(source, target)]["target_comp_id"] = vertexes[target].get("comp_id", None)
        edges[(source, target)]["target_comp_size"] = vertexes[target].get("comp_size", None)

        #if "comp_id" in vertexes[source]:
        #    sg[int(vertexes[source]["comp_id"])].add_edge(source, target)

        #if "comp_id" in vertexes[target]:
        #    sg[int(vertexes[target]["comp_id"])].add_edge(source, target)
        
        edges[(source, target)]["source_degree"] = vertexes[source].get("degree")
        edges[(source, target)]["target_degree"] = vertexes[target].get("degree")
        edges[(source, target)]["source_label"] = vertexes[source].get("label")
        edges[(source, target)]["target_label"] = vertexes[target].get("label")

    

    
def format_tag(v):
    if v > 0:
        return 1
    elif v < 0:
        return -1
    else:
        return 0


def calc_rate(d1, d2):
    if math.fabs(d1) < 0.000001 or math.fabs(d2) < 0.000001:
        return 0.0
    else:
        return d1/d2


def identify_node(delta_central_rate, delta_token_rate):

    if delta_central_rate > 0.2:
        return "1"
    if delta_token_rate < -0.2:
        return "-1"

    return "0"


def output_gephi_graph(sg_list, origin_sub, count_map, weight_map, react_weights, vertexes, edges, ext_info):
    with open("data/refine_edges.csv", "w") as edge_fout, open("data/refine_vertex.csv", "w") as vertex_fout:
        for cid in range(len(sg_list)):
            sg = sg_list[cid]
            sg_edges = sg.edges()
            sg_nodes = sg.nodes()

            if len(sg_nodes) < size_threshold or origin_sub[cid][1] < size_threshold:
                continue
            
            D = sg.degree()
            iD = sg.in_degree()
            oD = sg.out_degree()

            idc = nx.in_degree_centrality(sg)
            odc = nx.out_degree_centrality(sg)

            cw_map = {}

            for edge_key in sg_edges:
                edge = edges[edge_key]
                add_weight(edge["source"], edge["target"], cw_map, count_map[edge_key])

                try:
                    items = (
                        edge["timestamp"], 
                        edge["source"], 
                        edge["target"], 
                        "[%s]" % ",".join([ str(x) for x in edge["dtset"]]),
                        "<%s>" % ";".join(["[%s, %s]" % x for x in edge["timeset"]]),
                        edge["tag"],
                        str(D[edge["source"]]),
                        str(D[edge["target"]]),
                        str(edge["chain"]),
                        str(cid),
                        str(len(sg_nodes)),
                        str(weight_map[edge_key]),
                        str(react_weights[edge_key]),
                        "\t".join(ext_info[edge_key]),
                        )
                except Exception as e:
                    print(e)
                    print(edge)
                    return
                edge_fout.write("\t".join(items))
                edge_fout.write("\n")

            _delta_wight = 0.0
            _delta_central = 0.0
            for node in sg_nodes:
                if ("ALL", node) in cw_map and (node, "ALL") in cw_map:
                    _delta_wight += math.fabs(cw_map[("ALL", node)] - cw_map[(node, "ALL")])            
                    _delta_central += math.fabs(idc[node] -odc[node])
            _delta_central /= 2.0
            _delta_wight /= 2.0


            for node in sg_nodes:
                _central = idc[node] -odc[node]
                if ("ALL", node) in cw_map and (node, "ALL") in cw_map:
                    _token = cw_map[("ALL", node)] - cw_map[(node, "ALL")]
                else:
                    _token = 0.0
                delta_central_rate = calc_rate(_central, _delta_central)
                delta_token_rate = calc_rate(_token, _delta_wight)
                try:
                    items = (
                        node,
                        vertexes[node]["label"],
                        str(vertexes[node]["count"]),
                        str(vertexes[node]["abs"]),
                        vertexes[node]["tag"],
                        str(D[node]),
                        str(iD[node]),
                        str(oD[node]),
                        str(idc[node]),
                        str(odc[node]),
                        str(_central),
                        str(_delta_central),
                        str(cid),
                        str(len(sg_nodes)),
                        str(vertexes[node].get("comp_id")),
                        str(vertexes[node].get("comp_size")),
                        str(_token), 
                        str(_delta_wight),
                        str(max(math.fabs(delta_central_rate), math.fabs(delta_token_rate))),
                        identify_node(delta_central_rate, delta_token_rate),

                        )
                except Exception as e:
                    print(e)
                    print(node)
                    return
                vertex_fout.write("\t".join(items))
                vertex_fout.write("\n")

        
 
if __name__ == "__main__":
    exclude_nodes = load_exclude()
    raw_edges, ext_info = load_edges(exclude_nodes=exclude_nodes)
    _edges, _vertexes, sum_map, total_graph = add_gephi_timeset(raw_edges)
    subgraphs, originsub, edge_weight, react_weights = tag_vertex(_vertexes, sum_map, exclude_nodes, total_graph)

    #for s, t in edge_weight:
    #    if (_vertexes[s]["label"].startswith("grant") or _vertexes[s]["label"].startswith("application") ) and _vertexes[t]["label"].startswith("contributor"):
    #        print("\t".join([
    #            s, 
    #            t, 
    #            str(react_weights[(s,t)]),
    #            str(_vertexes[s].get("comp_id")),
    #            str(_vertexes[s].get("comp_size")),
    #            str(_vertexes[t].get("comp_id")),
    #            str(_vertexes[t].get("comp_size")),
    #            ]))
    tag_edge_and_vertex(_edges, _vertexes, subgraphs)
    output_gephi_graph(subgraphs, originsub, sum_map, edge_weight, react_weights, _vertexes, _edges, ext_info)
