chain_mapper = {
    "1": "ethereum",
    "56": "bsc",
    "137": "polygon",
    "10": "optimism",
    "100": "gnosis",
    "42161": "arbitrum",
    "43114": "avalanche-c",
    "250": "fantom",
    "128": "heco",
    "592": "astar",
}

scan_settings = {
    

    "zksync":{
        "endpoint": "https://api.zksync.io/api/v0.1",
        "api_key_list": [
            "FALSE_KEY"
        ],
        "account_history": "/account/{address}/history/newer_than?tx_id={tx_id}&limit=100",
        
    },
}

fetch_settings = {
    
    "alchemy_ethereum":{
        "api_key_list": [
            "FALESE_KEY",
        ],
        "jsonrpc_id": 1,
        "url_str": "https://eth-mainnet.alchemyapi.io/v2/{apikey}",
    },

    "alchemy_polygon":{
        "api_key_list": [
            "FALESE_KEY",
        ],
        "jsonrpc_id": 1,
        "url_str": "https://polygon-mainnet.g.alchemyapi.io/v2/{apikey}",
    },
}
