import streamlit as st
import pandas as pd
import requests
from streamlit_autorefresh import st_autorefresh
from web3 import Web3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import warnings

# Streamlit 스레드 경고 무시
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")

st.set_page_config(page_title="🔥 핫월렛 토큰 대시보드", layout="wide")

st_autorefresh(interval=180 * 1000, key="refresh")  # 3분마다 새로고침

st.title("🔥 체인별 핫월렛 토큰 실시간 대시보드")

# RPC URL을 chain에 따라 넣어줍니다.
RPC_URLS = {
    "ETH": [
        "https://eth.llamarpc.com",
        "https://ethereum.publicnode.com",
        "https://rpc.ankr.com/eth",
        "https://eth-mainnet.public.blastapi.io",
        "https://rpc.flashbots.net",
        "https://eth.drpc.org",
        "https://eth-mainnet.g.alchemy.com/v2/demo"
    ],
    "BSC": [
        "https://rpc.ankr.com/bsc",
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.binance.org",
        "https://bsc-dataseed2.binance.org",
        "https://bsc.publicnode.com"
    ],
    "ARB": [
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum.llamarpc.com",
        "https://arbitrum-one.publicnode.com",
        "https://rpc.ankr.com/arbitrum"
    ],
    "OP": [
        "https://mainnet.optimism.io",
        "https://optimism.llamarpc.com",
        "https://optimism.publicnode.com",
        "https://rpc.ankr.com/optimism"
    ],
    "BASE": [
        "https://mainnet.base.org",
        "https://base.llamarpc.com",
        "https://base.publicnode.com",
        "https://rpc.ankr.com/base"
    ],
    "AVAX": [
        "https://api.avax.network/ext/bc/C/rpc",
        "https://rpc.ankr.com/avalanche",
        "https://avalanche.publicnode.com",
        "https://avalanche-c-chain.publicnode.com"
    ],
    "POL": [
        "https://polygon-rpc.com",
        "https://polygon.llamarpc.com",
        "https://polygon.publicnode.com",
        "https://rpc.ankr.com/polygon"
    ]
}

# 백업 RPC URLs
BACKUP_RPC_URLS = {
    "ETH": "https://eth.llamarpc.com",
    "BSC": "https://bsc-dataseed.binance.org",
    "ARB": "https://arbitrum.llamarpc.com",
    "OP": "https://optimism.llamarpc.com",
    "BASE": "https://base.llamarpc.com",
    "AVAX": "https://rpc.ankr.com/avalanche",
    "POL": "https://polygon.llamarpc.com"
}

COINGECKO_CHAIN_MAP = {
    "ETH": "ethereum",
    "ARB": "arbitrum-one",
    "BSC": "binance-smart-chain",
    "BASE": "base",
    "OP": "optimistic-ethereum",
    "AVAX": "avalanche",
    "POL": "polygon-pos"
}

chain_info = {
    "ETH": {
        "explorer": "https://etherscan.io",
        "wallets": {
            "바낸16번핫": "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",
            "바낸15번핫": "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
            "바낸14번핫": "0x28c6c06298d514db089934071355e5743bf21d60",
            "바낸18번핫": "0x9696f59e4d72e237be84ffd425dcad154bf96976",
            "바낸28번지갑(콜드추정)": "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb",
            "게이트1번핫": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
            "바빗핫": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "코베10번핫": "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",
            "빗겟5번핫": "0x5bdf85216ec1e38d6458c870992a69e38e03f7ef",
            "빗겟4번핫": "0x0639556f03714a74a5feeaf5736a4a64ff70d206",
            "빗겟6번핫": "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23",            
            "오켁핫": "0x91d40e4818f4d4c57b4578d9eca6afc92ac8debe",
            "멕시16번핫": "0x9642b23ed1e01df1092b92641051881a322f5d4e",
            "쿠코핫": "0xd91efec7e42f80156d1d9f660a69847188950747",
            "쿠코20번핫": "0x58edf78281334335effa23101bbe3371b6a36a51",
            "코빗8번핫": "0xf0bc8fddb1f358cef470d63f96ae65b1d7914953",
            "코인원1번핫": "0x167a9333bf582556f35bd4d16a7e80e191aa6476",
            "후오비48번핫": "0xa03400e098f4421b34a3a44a1b4e571419517687",
            "크닷12번핫": "0x46340b20830761efd32832a74d7169b29feb9758",
            "빙핫": "0x065AC3d33FEC104FBa9f2f4D674AfAA7c4EBcF43",  
            "플립핫": "0xd49417f37cED33aBA35DDAbf208D5bFcD87b4eBe",
            "코캐핫": "0xFE6D9AF579dEcCeBfC2d8D366C3D667adB696b32",
            "비트마트16번핫": "0x2982bB64bcd07Ac3315C32Cb2BB7e5E8a2De7d67",
            "해시키2번핫": "0xcBEA7739929cc6A2B4e46A1F6D26841D8d668b9E",
            "비트파이넥스핫": "0x77134cbC06cB00b66F4c7e623D5fdBF6777635EC",
            "비트루핫": "0x6cc8dCbCA746a6E4Fdefb98E1d0DF903b107fd21",
            "크라켄7번핫": "0x89e51fA8CA5D66cd220bAed62ED01e8951aa7c40",
            "페멕스1번핫": "0xdb861e302ef7b7578a448e951aede06302936c28",
            "어센덱스6번핫": "0x983873529f95132BD1812A3B52c98Fb271d2f679",
            "제미니4번핫": "0x5f65f7b609678448494de4c87521cdf6cef1e932",
            "코인ex핫": "0x20145c5e27408b5c1cf2239d0115ee3bbc27cbd7",
            "woox2번핫": "0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4",
        },
        "rpc": "eth"
    },
    "BSC": {
        "explorer": "https://bscscan.com",
        "wallets": {
            "바낸12번핫": "0x515b72ed8a97f42c568d6a143232775018f133c8",
            "바낸16번핫": "0xa180fe01b906a1be37be6c534a3300785b20d947",
            "바빗핫": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "게이트핫": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
            "멕시13번핫": "0x4982085c9e2f89f2ecb8131eca71afad896e89cb",
            "비트루핫": "0x868f027a5e3bd1cd29606a6681c3ddb7d3dd9b67",
            "빗겟3번핫": "0x97b9d2102a9a65a26e1ee82d59e42d1b73b68689",
            "빗겟4번핫": "0x0639556f03714a74a5feeaf5736a4a64ff70d206",
            "빗겟6번핫": "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23",
            "쿠코2번핫": "0x53f78a071d04224b8e254e243fffc6d9f2f3fa23",
            "플립핫": "0xCD47f02B261426Ab734Be9271156327327407E43",
            "코캐핫": "0xFE6D9AF579dEcCeBfC2d8D366C3D667adB696b32",
            "오켁핫": "0xf5988713400DA6fC8A58EC9515e2b0DF9B40B115",
            "후오비72번핫": "0xdd3CB5c974601BC3974d908Ea4A86020f9999E0c",
            "빙핫": "0x065AC3d33FEC104FBa9f2f4D674AfAA7c4EBcF43",
            "비트마트핫": "0xa23EF2319bA4C933eBfDbA80c332664A6Cb13F1A",
            "해시키핫": "0x6A276a58C5194eF196B58442f627Dba070CB37BF",
            "페멕스핫": "0xDB861E302EF7B7578A448e951AedE06302936c28",
            "어센덱스핫": "0x983873529f95132BD1812A3B52c98Fb271d2f679",
            "코인ex핫": "0x32e3e876aa0C1732ed9Efcf9d8615De7afaEF59f",
            "woox 핫": "0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4",
        },
        "rpc": "bsc"
    },
    "ARB": {
        "explorer": "https://arbiscan.io",
        "wallets": {
            "바낸89번핫": "0x3931dab967c3e2dbb492fe12460a66d0fe4cc857",
            "바낸54번핫": "0xb38e8c17e38363af6ebdcb3dae12e0243582891d",
            "바낸핫3": "0x25681ab599b4e2ceea31f8b498052c53fc2d74db",
            "빗겟5번핫": "0x5bdf85216ec1e38d6458c870992a69e38e03f7ef",
            "게이트1번핫": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
            "바빗핫": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "플립6번핫": "0xa9b686EE77EfC18e7a08c48FA823CAA0cfDd754E",
            "오켁핫": "0xAfEE421482FAEa92292ED3ffE29371742542AD72",
            "쿠코24번핫": "0x03E6FA590CAdcf15A38e86158E9b3D06FF3399Ba",
        },
        "rpc": "arbitrum"
    },
    "OP": {
        "explorer": "https://optimistic.etherscan.io",
        "wallets": {
            "바낸55번핫": "0xacd03d601e5bb1b275bb94076ff46ed9d753435a",
            "바빗핫": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "게이트1번핫": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
            "멕시핫7번핫": "0xDF90C9B995a3b10A5b8570a47101e6c6a29eb945",
            "빗겟6번핫": "0x1AB4973a48dc892Cd9971ECE8e01DcC7688f8F23",
            "오켁핫": "0xB5216CB558Cb018583bED009EE25cA73Eb27bB1d",
            "쿠코26번핫": "0xa3f45e619cE3AAe2Fa5f8244439a66B203b78bCc",
            "코베11번핫": "0xC8373EDFaD6d5C5f600b6b2507F78431C5271fF5",
        },
        "rpc": "optimism"
    },
    "BASE": {
        "explorer": "https://basescan.org",
        "wallets": {
            "바낸73번핫": "0x3304e22ddaa22bcdc5fca2269b418046ae7b566a",
            "바빗6번핫": "0xbaed383ede0e5d9d72430661f3285daa77e9439f",
            "게이트1번핫": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
            "오켁핫": "0xc8802feab2fafb48b7d1ade77e197002c210f391",
        },
        "rpc": "base"
    },
    "AVAX": {
        "explorer": "https://snowtrace.io",
        "wallets": {
            "바낸1번핫": "0x6d8be5cdf0d7dee1f04e25fd70b001ae3b907824",
            "코베7번핫": "0xe1a0ddeb9b5b55e489977b438764e60e314e917c",
            "코베1번핫": "0x3dd87411a3754deea8cc52c4cf57e2fc254924cc",
            "오켁핫": "0xC94bb9b883Ab642C1C3Ed07af4E36523e7DaF1Fe",
            "쿠코32번핫": "0x4E75e27e5Aa74F0c7A9D4897dC10EF651f3A3995",
            "훠비핫": "0xa77ff0e1C52f58363a53282624C7BaA5fA91687D",
            "빗겟6번핫": "0x1AB4973a48dc892Cd9971ECE8e01DcC7688f8F23",
        },
        "rpc": "avalanche"
    },
    "POL": {
        "explorer": "https://polygonscan.com",
        "wallets": {
            "바빗핫": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "멕시핫": "0x51E3D44172868Acc60D68ca99591Ce4230bc75E0",
            "빗겟6번핫": "0x1AB4973a48dc892Cd9971ECE8e01DcC7688f8F23",
            "빗겟4번핫": "0x0639556F03714A74a5fEEaF5736a4A64fF70D206",

            "쿠코핫": "0x9AC5637d295FEA4f51E086C329d791cC157B1C84",
            "오켁핫": "0x343d752bB710c5575E417edB3F9FA06241A4749A",
        },
        "rpc": "polygon"
    },
}

selected_chain = st.selectbox("체인을 선택하세요", list(chain_info.keys()))
token_input = st.text_input("토큰 티커 or 컨트랙트트 주소 입력 (0x로 시작)")

# DEX 풀 포함 옵션
include_dex = st.checkbox("DEX 유동성 풀 포함 (베타)", value=False)
if include_dex:
    st.info("⚠️ DEX 유동성 풀 조회는 베타 기능입니다. 주요 DEX의 페어를 표시합니다.")

# 병렬처리 워커 수 설정
col1, col2 = st.columns([3, 1])
with col2:
    max_workers = st.slider("병렬처리 워커 수", min_value=1, max_value=10, value=5, help="동시에 처리할 작업 수")

# 토큰 정보를 저장할 전역 변수 (thread-safe)
token_info_lock = threading.Lock()
token_info_cache = {}

def get_dexscreener_data(chain_id, token_address):
    """DexScreener API를 통해 DEX 유동성 정보 조회"""
    chain_map = {
        "ETH": "ethereum",
        "BSC": "bsc",
        "ARB": "arbitrum",
        "OP": "optimism",
        "BASE": "base",
        "AVAX": "avalanche",
        "POL": "polygon"
    }
    
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        headers = {"Accept": "application/json"}
        
        res = requests.get(url, headers=headers, timeout=10)
        
        if res.status_code == 200:
            data = res.json()
            pairs = data.get("pairs", [])
            
            chain_name = chain_map.get(chain_id)
            filtered_pairs = [p for p in pairs if p.get("chainId") == chain_name]
            filtered_pairs.sort(key=lambda x: float(x.get("liquidity", {}).get("usd", 0)), reverse=True)
            
            return filtered_pairs[:5]
        
        return []
        
    except Exception as e:
        return []

def format_liquidity_info(pairs):
    """DEX 유동성 정보를 포맷팅"""
    liquidity_info = []
    
    for pair in pairs:
        dex_name = pair.get("dexId", "Unknown").upper()
        pair_address = pair.get("pairAddress", "")
        liquidity_usd = float(pair.get("liquidity", {}).get("usd", 0))
        base_token = pair.get("baseToken", {})
        quote_token = pair.get("quoteToken", {})
        
        token_liquidity_usd = liquidity_usd / 2
        
        info = {
            "name": f"🏊 {dex_name} ({quote_token.get('symbol', 'UNKNOWN')} 페어)",
            "address": pair_address,
            "liquidity_usd": token_liquidity_usd,
            "price_usd": float(pair.get("priceUsd", 0)),
            "volume_24h": float(pair.get("volume", {}).get("h24", 0))
        }
        
        liquidity_info.append(info)
    
    return liquidity_info

def get_working_rpc(rpc_urls, chain_name):
    """작동하는 RPC를 찾아 반환"""
    import random
    
    # ETH 체인은 더 많은 RPC 옵션 제공
    if chain_name == "ETH":
        eth_extra_rpcs = [
            "https://1rpc.io/eth",
            "https://eth.meowrpc.com",
            "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
            "https://endpoints.omniatech.io/v1/eth/mainnet/public"
        ]
        rpc_urls = rpc_urls + eth_extra_rpcs
    
    rpc_list = rpc_urls.copy()
    random.shuffle(rpc_list)
    
    for rpc in rpc_list:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 5}))
            if w3.is_connected():
                # 블록 번호 조회로 실제 작동 확인
                block_num = w3.eth.block_number
                if block_num > 0:
                    return w3
        except:
            continue
            
    return None

def get_token_info(w3, token_contract):
    """토큰의 이름, 심볼, decimals를 조회하는 함수"""
    global token_info_cache
    
    # 캐시 확인
    with token_info_lock:
        if token_contract in token_info_cache:
            return token_info_cache[token_contract]
    
    token_info = {
        "name": "Unknown",
        "symbol": "Unknown",
        "decimals": 18
    }
    
    # 표준 ERC20 ABI
    standard_abi = [
        {"constant": True, "inputs": [], "name": "name", "outputs": [{"type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"type": "string"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"type": "uint8"}], "type": "function"}
    ]
    
    try:
        contract = w3.eth.contract(address=w3.to_checksum_address(token_contract), abi=standard_abi)
        
        # Name 조회
        try:
            token_info["name"] = contract.functions.name().call()
        except:
            # bytes32 형태로 시도
            try:
                name_abi = [{"constant": True, "inputs": [], "name": "name", "outputs": [{"type": "bytes32"}], "type": "function"}]
                name_contract = w3.eth.contract(address=w3.to_checksum_address(token_contract), abi=name_abi)
                result = name_contract.functions.name().call()
                if result:
                    token_info["name"] = result.decode('utf-8').rstrip('\x00') if isinstance(result, bytes) else Web3.to_text(result).rstrip('\x00')
            except:
                pass
        
        # Symbol 조회
        try:
            token_info["symbol"] = contract.functions.symbol().call()
        except:
            # bytes32 형태로 시도
            try:
                symbol_abi = [{"constant": True, "inputs": [], "name": "symbol", "outputs": [{"type": "bytes32"}], "type": "function"}]
                symbol_contract = w3.eth.contract(address=w3.to_checksum_address(token_contract), abi=symbol_abi)
                result = symbol_contract.functions.symbol().call()
                if result:
                    token_info["symbol"] = result.decode('utf-8').rstrip('\x00') if isinstance(result, bytes) else Web3.to_text(result).rstrip('\x00')
            except:
                pass
        
        # Decimals 조회
        try:
            token_info["decimals"] = contract.functions.decimals().call()
        except:
            pass
            
    except Exception as e:
        st.warning(f"토큰 정보 조회 중 오류: {str(e)}")
    
    # 캐시 저장
    with token_info_lock:
        token_info_cache[token_contract] = token_info
        
    return token_info

def get_token_balance_rpc(rpc_urls, wallet, token_contract, wallet_name=None, chain_name=None):
    """RPC를 통해 토큰 잔고를 조회하는 함수"""
    import random
    
    # RPC URL 리스트를 무작위로 섞어서 부하 분산
    rpc_list = rpc_urls.copy() if isinstance(rpc_urls, list) else [rpc_urls]
    random.shuffle(rpc_list)
    
    last_error = None
    
    for rpc in rpc_list:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 30}))
            
            if not w3.is_connected():
                last_error = f"RPC 연결 실패: {rpc}"
                time.sleep(0.2)  # 짧은 대기
                continue
                
            # 토큰 정보 조회
            token_info = get_token_info(w3, token_contract)
            decimals = token_info["decimals"]
            
            # 잔고 조회
            contract = w3.eth.contract(address=w3.to_checksum_address(token_contract), abi=[{
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            }])
            
            raw_balance = contract.functions.balanceOf(w3.to_checksum_address(wallet)).call()
            balance = raw_balance / (10 ** decimals)
            
            return {"wallet_name": wallet_name, "wallet": wallet, "balance": round(balance, 4), "error": None}
            
        except Exception as e:
            last_error = str(e)
            if "429" in str(e):  # Rate limit error
                time.sleep(1)  # 더 긴 대기
            else:
                time.sleep(0.3)  # 일반 오류시 짧은 대기
            continue
    
    # 모든 RPC 실패시 0 반환
    return {"wallet_name": wallet_name, "wallet": wallet, "balance": 0, "error": None}

def get_token_price_from_1inch(chain_id, contract_address):
    """1inch API를 통해 토큰 가격을 조회하는 함수"""
    chain_id_map = {
        "ETH": 1,
        "BSC": 56,
        "ARB": 42161,
        "OP": 10,
        "BASE": 8453,
        "AVAX": 43114,  
        "POL": 137
    }
    
    try:
        url = f"https://api.1inch.io/v5.0/{chain_id_map.get(chain_id, 1)}/quote"
        
        usdc_addresses = {
            "ETH": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "BSC": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
            "ARB": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            "OP": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
            "BASE": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "AVAX": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",    
            "POL": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
        }
        
        params = {
            "fromTokenAddress": contract_address,
            "toTokenAddress": usdc_addresses.get(chain_id, usdc_addresses["ETH"]),
            "amount": str(10**18)
        }
        
        headers = {"Accept": "application/json"}
        
        res = requests.get(url, params=params, headers=headers, timeout=10)
        
        if res.status_code == 200:
            data = res.json()
            price = int(data.get("toTokenAmount", 0)) / 10**6
            return price
        else:
            return 0
            
    except Exception as e:
        return 0

def get_token_price(chain_key, contract_address, selected_chain=None):
    """토큰 가격을 조회하는 함수 (CoinGecko 우선, 실패시 1inch)"""
    price_source = "CoinGecko"
    
    # 1. CoinGecko 시도
    try:
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{chain_key}"
        params = {
            "contract_addresses": contract_address.lower(),
            "vs_currencies": "usd"
        }
        headers = {"Accept": "application/json"}
        res = requests.get(url, params=params, headers=headers, timeout=10)
        
        if res.status_code == 200:
            data = res.json()
            price = data.get(contract_address.lower(), {}).get("usd", 0)
            if price > 0:
                return price, price_source
        
    except Exception as e:
        pass
    
    # 2. CoinGecko 실패시 1inch API 시도
    if selected_chain:
        price = get_token_price_from_1inch(selected_chain, contract_address)
        if price > 0:
            price_source = "1inch (DEX)"
            return price, price_source
    
    return 0, "없음"

if token_input.startswith("0x") and selected_chain:
    # 토큰 정보 캐시 초기화
    token_info_cache.clear()
        
    wallets = chain_info[selected_chain]["wallets"]
    explorer = chain_info[selected_chain]["explorer"]
    cg_key = COINGECKO_CHAIN_MAP.get(selected_chain, "ethereum")
    rpc_urls = RPC_URLS.get(selected_chain)
    backup_rpc_url = BACKUP_RPC_URLS.get(selected_chain)
    
    # 토큰 정보 먼저 조회 (첫 번째 지갑으로)
    with st.spinner('토큰 정보 조회 중...'):
        try:
            # 작동하는 RPC 찾기
            w3 = get_working_rpc(rpc_urls, selected_chain)
            
            if w3 and w3.is_connected():
                token_info = get_token_info(w3, token_input)
                
                # 토큰 정보가 Unknown인 경우 다른 RPC로 재시도
                if token_info["name"] == "Unknown" or token_info["symbol"] == "Unknown":
                    st.info(f"토큰 정보 조회 재시도 중... (체인: {selected_chain})")
                    
                    # 캐시 클리어하고 다시 시도
                    with token_info_lock:
                        if token_input in token_info_cache:
                            del token_info_cache[token_input]
                    
                    # 다른 RPC로 재시도
                    for rpc in rpc_urls:
                        try:
                            w3_retry = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 10}))
                            if w3_retry.is_connected():
                                token_info = get_token_info(w3_retry, token_input)
                                if token_info["name"] != "Unknown":
                                    break
                        except:
                            continue
                
                token_display_name = f"{token_info['name']} ({token_info['symbol']})"
                st.subheader(f"📊 {selected_chain} 체인 - {token_display_name}")
                
                # 토큰 정보 박스
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.info(f"**토큰 이름:** {token_info['name']}")
                with col2:
                    st.info(f"**심볼:** {token_info['symbol']}")
                with col3:
                    st.info(f"**컨트랙트:** {token_input[:10]}...{token_input[-6:]}")
            else:
                st.warning(f"⚠️ {selected_chain} 체인 RPC 연결 실패. 토큰 정보를 가져올 수 없습니다.")
                st.subheader(f"📊 {selected_chain} 체인 - {token_input[:10]}...{token_input[-6:]} 잔고")
        except Exception as e:
            st.error(f"토큰 정보 조회 오류: {str(e)}")
            st.subheader(f"📊 {selected_chain} 체인 - {token_input[:10]}...{token_input[-6:]} 잔고")
    
    # 토큰 가격 조회
    with st.spinner('토큰 가격 조회 중...'):
        token_price, price_source = get_token_price(cg_key, token_input, selected_chain)
        if token_price > 0:
            st.success(f"토큰 가격: ${token_price:,.6f} (출처: {price_source})")
        else:
            st.warning("토큰 가격을 가져올 수 없습니다.")
    
    # 정렬 옵션
    sort_option = st.radio("정렬 기준", ["잔고 많은 순", "달러 가치 높은 순"], horizontal=True)
    
    # 병렬 잔고 조회
    with st.spinner(f'잔고 조회 중... (병렬처리: {max_workers}개 워커)'):
        rows = []
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        try:
            # ThreadPoolExecutor를 사용한 병렬처리
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # CEX 핫월렛 조회 작업 제출
                future_to_wallet = {}
                for name, addr in wallets.items():
                    future = executor.submit(get_token_balance_rpc, rpc_urls, addr, token_input, name, selected_chain)
                    future_to_wallet[future] = (name, addr)
                
                # 완료된 작업 처리
                completed = 0
                total = len(wallets)
                
                for future in as_completed(future_to_wallet, timeout=60):  # 60초 타임아웃 추가
                    try:
                        name, addr = future_to_wallet[future]
                        result = future.result(timeout=5)  # 개별 작업 타임아웃
                        
                        balance = result["balance"]
                        usd_value = round(balance * token_price, 2) if token_price > 0 else 0
                        token_url = f"{explorer}/token/{token_input}?a={addr}"
                        
                        rows.append({
                            "지갑이름": name,
                            "주소": addr[:10] + "..." + addr[-6:],
                            "잔고": f"{balance:,.4f}",
                            "달러환산": f"${usd_value:,.2f}",
                            "탐색기": token_url,
                            "가격출처": price_source if token_price > 0 else "-",
                            "타입": "CEX"
                        })
                        
                        # 진행률 업데이트
                        completed += 1
                        progress_bar.progress(completed / total)
                        progress_text.text(f"진행 중: {completed}/{total} 지갑 완료")
                        
                    except Exception as e:
                        # 오류 발생시에도 0으로 추가
                        name, addr = future_to_wallet[future]
                        rows.append({
                            "지갑이름": name,
                            "주소": addr[:10] + "..." + addr[-6:],
                            "잔고": "0.0000",
                            "달러환산": "$0.00",
                            "탐색기": f"{explorer}/token/{token_input}?a={addr}",
                            "가격출처": "-",
                            "타입": "CEX"
                        })
                        completed += 1
                        progress_bar.progress(completed / total)
                        
        except Exception as e:
            st.error(f"전체 오류 발생: {str(e)}")
        finally:
            progress_bar.empty()
            progress_text.empty()
        
        # DEX 유동성 풀 조회 (옵션 선택시)
        if include_dex:
            st.info("🔍 DEX 유동성 풀 조회 중...")
            dex_pairs = get_dexscreener_data(selected_chain, token_input)
            
            if dex_pairs:
                liquidity_infos = format_liquidity_info(dex_pairs)
                
                for liq_info in liquidity_infos:
                    token_amount = liq_info["liquidity_usd"] / liq_info["price_usd"] if liq_info["price_usd"] > 0 else 0
                    
                    rows.append({
                        "지갑이름": liq_info["name"],
                        "주소": liq_info["address"][:10] + "..." + liq_info["address"][-6:] if liq_info["address"] else "N/A",
                        "잔고": f"{token_amount:,.4f}",
                        "달러환산": f"${liq_info['liquidity_usd']:,.2f}",
                        "탐색기": f"{explorer}/address/{liq_info['address']}" if liq_info["address"] else "#",
                        "가격출처": "DexScreener",
                        "타입": "DEX"
                    })
                
                if liquidity_infos:
                    total_volume = sum(info["volume_24h"] for info in liquidity_infos)
                    st.success(f"📊 DEX 24시간 거래량: ${total_volume:,.2f}")
            else:
                st.warning("DEX 유동성 풀을 찾을 수 없습니다.")
        
        progress_bar.empty()
        progress_text.empty()
    
    # 데이터프레임 표시
    df = pd.DataFrame(rows)
    
    # 정렬을 위해 숫자 형태의 컬럼 추가
    df['잔고_숫자'] = df['잔고'].str.replace(',', '').astype(float)
    df['달러_숫자'] = df['달러환산'].str.replace('$', '').str.replace(',', '').astype(float)
    
    # 정렬
    if sort_option == "잔고 많은 순":
        df = df.sort_values('잔고_숫자', ascending=False)
    else:
        df = df.sort_values('달러_숫자', ascending=False)
    
    # 숫자 컬럼 제거
    df = df.drop(['잔고_숫자', '달러_숫자'], axis=1)
    
    # 총계 표시
    col1, col2, col3 = st.columns(3)
    
    # CEX와 DEX 분리 계산
    cex_rows = [r for r in rows if r.get("타입") == "CEX"]
    dex_rows = [r for r in rows if r.get("타입") == "DEX"]
    
    cex_balance = sum(float(row["잔고"].replace(",", "")) for row in cex_rows)
    cex_usd = sum(float(row["달러환산"].replace("$", "").replace(",", "")) for row in cex_rows)
    
    dex_balance = sum(float(row["잔고"].replace(",", "")) for row in dex_rows)
    dex_usd = sum(float(row["달러환산"].replace("$", "").replace(",", "")) for row in dex_rows)
    
    with col1:
        st.metric("CEX 총 잔고", f"{cex_balance:,.4f}")
        if include_dex and dex_rows:
            st.metric("DEX 총 잔고", f"{dex_balance:,.4f}")
    with col2:
        st.metric("CEX 달러 가치", f"${cex_usd:,.2f}")
        if include_dex and dex_rows:
            st.metric("DEX 달러 가치", f"${dex_usd:,.2f}")
    with col3:
        total_balance = cex_balance + dex_balance
        total_usd = cex_usd + dex_usd
        st.metric("전체 총 잔고", f"{total_balance:,.4f}")
        st.metric("전체 달러 가치", f"${total_usd:,.2f}")
    
    # 테이블 표시
    st.dataframe(
        df,
        use_container_width=True,
        height=min(len(df) * 40 + 100, 1000),
        column_config={
            "탐색기": st.column_config.LinkColumn(
                "탐색기",
                help="블록 탐색기에서 확인",
                display_text="🔍 확인"
            ),
            "가격출처": st.column_config.TextColumn(
                "가격출처",
                help="가격 데이터 제공처"
            ),
            "타입": st.column_config.TextColumn(
                "타입",
                help="CEX 또는 DEX"
            )
        }
    )
    
    # 디버깅 정보
    with st.expander("디버깅 정보"):
        st.write(f"체인: {selected_chain}")
        st.write(f"RPC URLs: {len(rpc_urls)} 개")
        st.write(f"토큰 주소: {token_input}")
        st.write(f"토큰 가격: ${token_price} (출처: {price_source})")
        st.write(f"병렬처리 워커 수: {max_workers}")
        if token_info_cache:
            st.write(f"토큰 정보: {token_info_cache.get(token_input, 'N/A')}")
        
else:
    st.info("정확한 토큰 **컨트랙트 주소**를 입력하세요 (0x로 시작)")
    
    # 예시 토큰 표시
    st.subheader("📌 예시 토큰 주소")
    
    example_tokens = {
        "ETH": {
            "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "SHIB": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
            "PEPE": "0x6982508145454ce325ddbe47a25d4ec3d2311933"
        },
        "BSC": {
            "USDT": "0x55d398326f99059ff775485246999027b3197955",
            "BUSD": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
            "CAKE": "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"
        },
        "ARB": {
            "USDC": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            "USDT": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
            "ARB": "0x912ce59144191c1204e64559fe8253a0e49e6548"
        },
        "OP": {
            "USDC": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
            "USDT": "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",
            "OP": "0x4200000000000000000000000000000000000042"
        },
        "BASE": {
            "USDC": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "USDbC": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
            "BRETT": "0x532f27101965dd16442e59d40670faf5ebb142e4"
        },
        "AVAX": {
            "USDT": "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7",
            "USDC": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
            "WAVAX": "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
        },
        "POL": {
            "USDC": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            "USDT": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
            "MATIC": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
        }
    }
    
    if selected_chain in example_tokens:
        st.write(f"{selected_chain} 체인 예시:")
        for token_name, token_addr in example_tokens[selected_chain].items():
            st.code(f"{token_name}: {token_addr}")
