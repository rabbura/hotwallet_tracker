import streamlit as st
import pandas as pd
import requests
from streamlit_autorefresh import st_autorefresh
from web3 import Web3
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import warnings

# Streamlit 스레드 경고 무시
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")

st.set_page_config(page_title="🔥 핫월렛 토큰 대시보드", layout="wide")

st_autorefresh(interval=180 * 1000, key="refresh")  # 3분마다 새로고침

st.title("🔥 체인별 핫월렛 토큰 실시간 대시보드")

# ============================================================
# Explorer API 키 설정 (Streamlit Secrets 사용)
# ============================================================
# Streamlit Cloud에서는 Settings > Secrets에 아래 형식으로 추가:
# [explorer_api_keys]
# ETH = "your_etherscan_api_key"
# BSC = "your_bscscan_api_key"
# ARB = "your_arbiscan_api_key"
# OP = "your_optimistic_etherscan_api_key"
# BASE = "your_basescan_api_key"
# AVAX = "your_snowtrace_api_key"
# POL = "your_polygonscan_api_key"

def get_explorer_api_key(chain):
    """Explorer API 키 가져오기"""
    try:
        return st.secrets.get("explorer_api_keys", {}).get(chain, "")
    except:
        return ""

# Explorer API URL 매핑 (Etherscan API V2 사용)
# V2는 단일 엔드포인트 + chainid 파라미터 방식
ETHERSCAN_V2_API = "https://api.etherscan.io/v2/api"

# 체인별 Chain ID (Etherscan V2용)
CHAIN_IDS = {
    "ETH": 1,
    "BSC": 56,
    "ARB": 42161,
    "OP": 10,
    "BASE": 8453,
    "POL": 137
}

# AVAX는 별도 API 사용 (Routescan)
AVAX_API_URL = "https://api.routescan.io/v2/network/mainnet/evm/43114/etherscan/api"

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
            "바낸15번핫(거의메인)": "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
            "바낸14번핫(거의메인)": "0x28c6c06298d514db089934071355e5743bf21d60",
            "바낸18번핫": "0x9696f59e4d72e237be84ffd425dcad154bf96976",
            "바낸51번지갑(콜드추정)": "0x8894e0a0c962cb723c1976a4421c95949be2d4e3",
            "바낸93번지갑(콜드추정)": "0x98adef6f2ac8572ec48965509d69a8dd5e8bba9d",
            "바낸withdraw7지갑": "0xe2fc31f816a9b94326492132018c3aecc4a93ae1",
            "바낸콜드1": "0x0a367f918340d47d36b21c93e9a2b6853cc9d6f0",
            "바낸콜드2": "0x6b5c22a67b44faac1eddd6ae1b284b2606f62071",
            "바낸28번지갑(콜드추정)": "0x5a52E96BAcdaBb82fd05763E25335261B270Efcb",
            "바낸20번지갑(콜드추정)": "0xF977814e90dA44bFA03b6295A0616a897441aceC",
            "게이트1번핫(거의메인)": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
            "게이트콜드추정": "0xD13C536e71698e189329e9583BE8b67817E045b0",
            "바빗핫(거의메인)": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "바빗핫2": "0xA31231E727Ca53Ff95f0D00a06C645110c4aB647",
            "바빗핫3": "0xad85405cbb1476825b78a021fa9e543bf7937549",
            "바빗핫4": "0x6522B7F9d481eCEB96557F44753a4b893F837E90",
            "바빗핫173(거의메인)": "0xf42aac93ab142090db9fdc0bc86aab73cb36f173",
            "코베10번핫(거의메인)": "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43",
            "빗겟3번핫": "0x97b9D2102A9a65A26E1EE82D59e42d1B73B68689",
            "빗겟5번핫": "0x5bdf85216ec1e38d6458c870992a69e38e03f7ef",
            "빗겟4번핫": "0x0639556f03714a74a5feeaf5736a4a64ff70d206",
            "빗겟6번핫(거의메인)": "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23",
            "오켁핫": "0x91d40e4818f4d4c57b4578d9eca6afc92ac8debe",
            "오켁146지갑": "0x4a4aaa0155237881fbd5c34bfae16e985a7b068d",
            "오켁콜드": "0xdce83237fbf279c4522e7cac4b10428e2b8694da",
            "멕시16번핫(거의메인)": "0x9642b23ed1e01df1092b92641051881a322f5d4e",
            "쿠코핫": "0xd91efec7e42f80156d1d9f660a69847188950747",
            "쿠코20번핫(거의메인)": "0x58edf78281334335effa23101bbe3371b6a36a51",
            "코빗8번핫": "0xf0bc8fddb1f358cef470d63f96ae65b1d7914953",
            "코인원1번핫": "0x167a9333bf582556f35bd4d16a7e80e191aa6476",
            "후오비48번핫(거의메인)": "0xa03400e098f4421b34a3a44a1b4e571419517687",
            "후오비60번핫(콜드추정)": "0x4fb312915B779b1339388e14b6d079741Ca83128",
            "크닷12번핫": "0x46340b20830761efd32832a74d7169b29feb9758",
            "크닷21번핫": "0x5b71d5fd6bb118665582dd87922bf3b9de6c75f9",
            "빙핫": "0x065AC3d33FEC104FBa9f2f4D674AfAA7c4EBcF43",
            "플립핫": "0xd49417f37cED33aBA35DDAbf208D5bFcD87b4eBe",
            "코캐핫": "0xFE6D9AF579dEcCeBfC2d8D366C3D667adB696b32",
            "비트마트16번핫": "0x2982bB64bcd07Ac3315C32Cb2BB7e5E8a2De7d67",
            "해시키2번핫": "0xcBEA7739929cc6A2B4e46A1F6D26841D8d668b9E",
            "비트파이넥스핫": "0x77134cbC06cB00b66F4c7e623D5fdBF6777635EC",
            "비트루핫": "0x6cc8dCbCA746a6E4Fdefb98E1d0DF903b107fd21",
            "크라켄7번핫": "0x89e51fA8CA5D66cd220bAed62ED01e8951aa7c40",
            "크라켄28번핫": "0x5c5F75B6FbA2903ADf66C7bDdCeA99B4CcE44a8A",
            "페멕스1번핫": "0xdb861e302ef7b7578a448e951aede06302936c28",
            "어센덱스6번핫": "0x983873529f95132BD1812A3B52c98Fb271d2f679",
            "제미니4번핫": "0x5f65f7b609678448494de4c87521cdf6cef1e932",
            "코인ex핫": "0x20145c5e27408b5c1cf2239d0115ee3bbc27cbd7",
            "고팍스핫": "0xe3031c1bfaa7825813c562cbdcc69d96fcad2087",
            "woox2번핫": "0x63DFE4e34A3bFC00eB0220786238a7C6cEF8Ffc4",
        },
        "rpc": "eth"
    },
    "BSC": {
        "explorer": "https://bscscan.com",
        "wallets": {
            "바낸12번핫": "0x515b72ed8a97f42c568d6a143232775018f133c8",
            "바낸10번핫": "0xEB2d2F1b8c558a40207669291Fda468E50c8A0bB",
            "바낸20번핫(콜드추정)": "0xF977814e90dA44bFA03b6295A0616a897441aceC",
            "바낸16번핫": "0xa180fe01b906a1be37be6c534a3300785b20d947",
            "바낸7번핫": "0xe2fc31F816A9b94326492132018C3aEcC4a93aE1",
            "바낸51번핫": "0x8894E0a0c962CB723c1976a4421c95949bE2D4E3",
            "바빗핫(거의메인)": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
            "바빗핫2": "0xc3121c4ca7402922e025e62e9bb4d5b244303878",
            "게이트핫(거의메인)": "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
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
            "바빗핫(거의메인)": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
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
            "바빗핫(거의메인)": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
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
            "멕시15번": "0x4e3ae00E8323558fA5Cac04b152238924AA31B60",
            "빗겟3번핫": "0x97b9D2102A9a65A26E1EE82D59e42d1B73B68689",
            "오켁핫": "0xc8802feab2fafb48b7d1ade77e197002c210f391",
        },
        "rpc": "base"
    },
    "AVAX": {
        "explorer": "https://snowtrace.io",
        "wallets": {
            "바낸1번핫": "0x6d8be5cdf0d7dee1f04e25fd70b001ae3b907824",
            "바낸핫": "0xcddc5d0ebeb71a08fff26909aa6c0d4e256b4fe1",
            "바낸핫2": "0x3bce63c6c9abf7a47f52c9a3a7950867700b0158",
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
            "바빗핫(거의메인)": "0xf89d7b9c864f589bbf53a82105107622b35eaa40",
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
token_input = st.text_input("토큰 티커 or 컨트랙트 주소 입력 (0x로 시작)")

# DEX 풀 포함 옵션
include_dex = st.checkbox("DEX 유동성 풀 포함 (베타)", value=False)
if include_dex:
    st.info("⚠️ DEX 유동성 풀 조회는 베타 기능입니다. 주요 DEX의 페어를 표시합니다.")

# 병렬처리 워커 수 설정
col1, col2 = st.columns([3, 1])
with col2:
    max_workers = st.slider("병렬처리 워커 수", min_value=1, max_value=10, value=4, help="동시에 처리할 작업 수")

# 토큰 정보를 저장할 전역 변수 (thread-safe)
token_info_lock = threading.Lock()
token_info_cache = {}

# ============================================================
# 시간 포맷팅 함수 (몇 분 전, 몇 시간 전 등)
# ============================================================
def format_time_ago(timestamp):
    """타임스탬프를 '몇 분 전' 형식으로 변환"""
    if not timestamp:
        return "-"

    now = datetime.now()
    tx_time = datetime.fromtimestamp(timestamp)
    diff = now - tx_time

    seconds = diff.total_seconds()

    if seconds < 60:
        return f"{int(seconds)}초 전"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"{minutes}분 전"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"{hours}시간 전"
    else:
        days = int(seconds / 86400)
        return f"{days}일 전"

def format_amount(amount):
    """금액을 K, M 단위로 포맷팅"""
    if amount is None:
        return "-"
    if amount >= 1_000_000:
        return f"{amount/1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"{amount/1_000:.2f}K"
    else:
        return f"{amount:.2f}"

# ============================================================
# 최근 출금 정보 조회 함수 (Etherscan API V2 사용)
# ============================================================
def get_last_withdrawal(chain, wallet, token_contract, decimals=18):
    """
    해당 지갑에서 특정 토큰의 최근 출금(전송) 정보 조회

    Returns:
        dict: {
            "amount": 출금 금액,
            "to": 받는 주소,
            "timestamp": 출금 시간 (unix timestamp),
            "tx_hash": 트랜잭션 해시
        }
        또는 None (조회 실패/없음)
    """
    api_key = get_explorer_api_key(chain)

    # AVAX는 별도 API 사용
    if chain == "AVAX":
        api_url = AVAX_API_URL
        params = {
            "module": "account",
            "action": "tokentx",
            "address": wallet,
            "contractaddress": token_contract,
            "page": 1,
            "offset": 30,
            "sort": "desc"
        }
        if api_key:
            params["apikey"] = api_key
    else:
        # Etherscan V2 API 사용
        chain_id = CHAIN_IDS.get(chain)
        if not chain_id:
            return None

        api_url = ETHERSCAN_V2_API
        params = {
            "chainid": chain_id,
            "module": "account",
            "action": "tokentx",
            "address": wallet,
            "contractaddress": token_contract,
            "page": 1,
            "offset": 30,
            "sort": "desc"
        }
        if api_key:
            params["apikey"] = api_key

    max_retries = 3

    # Rate limit 방지: API 호출 전 약간의 딜레이
    time.sleep(0.15)

    for attempt in range(max_retries):
        try:
            res = requests.get(api_url, params=params, timeout=15)

            if res.status_code == 200:
                data = res.json()

                # API 에러 처리 (Rate limit, NOTOK 등)
                if data.get("status") == "0":
                    error_msg = data.get("message", "Unknown error").lower()
                    # Rate limit 또는 NOTOK은 재시도
                    if "rate limit" in error_msg or "notok" in error_msg:
                        if attempt < max_retries - 1:
                            time.sleep(1.5 + attempt)  # 점점 더 긴 대기 (1.5초, 2.5초, 3.5초...)
                            continue
                    # 최종 실패시 에러 반환
                    return {"error": data.get("message", "Unknown error"), "wallet": wallet[:10]}

                if data.get("status") == "1" and data.get("result"):
                    result_list = data.get("result", [])

                    # 트랜잭션이 없는 경우
                    if len(result_list) == 0:
                        return {"error": "No transactions", "wallet": wallet[:10]}

                    # 출금 트랜잭션만 필터링 (from == wallet)
                    out_count = 0
                    for tx in result_list:
                        tx_from = tx.get("from", "").lower()
                        wallet_lower = wallet.lower()

                        if tx_from == wallet_lower:
                            out_count += 1
                            token_decimal = int(tx.get("tokenDecimal", decimals))
                            amount = int(tx.get("value", 0)) / (10 ** token_decimal)

                            return {
                                "amount": amount,
                                "to": tx.get("to", ""),
                                "timestamp": int(tx.get("timeStamp", 0)),
                                "tx_hash": tx.get("hash", "")
                            }

                    # 출금이 없는 경우 (입금만 있음)
                    return {"error": f"No OUT in {len(result_list)} txs", "wallet": wallet[:10]}

            return {"error": f"HTTP {res.status_code}", "wallet": wallet[:10]}

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.3)
                continue
            return {"error": str(e)[:50], "wallet": wallet[:10]}

    return None

# ============================================================
# 잔고 + 출금 정보 통합 조회 함수
# ============================================================
def get_wallet_data(rpc_urls, wallet, token_contract, wallet_name, chain_name, decimals=18):
    """
    지갑의 잔고와 최근 출금 정보를 함께 조회

    Returns:
        dict: {
            "wallet_name": 지갑 이름,
            "wallet": 지갑 주소,
            "balance": 잔고,
            "last_withdrawal": 최근 출금 정보 dict 또는 None,
            "error": 에러 메시지 또는 None
        }
    """
    import random

    result = {
        "wallet_name": wallet_name,
        "wallet": wallet,
        "balance": 0,
        "last_withdrawal": None,
        "error": None
    }

    # 1. 잔고 조회 (RPC)
    rpc_list = rpc_urls.copy() if isinstance(rpc_urls, list) else [rpc_urls]
    random.shuffle(rpc_list)

    balance_success = False
    for rpc in rpc_list:
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 15}))

            if not w3.is_connected():
                continue

            # 토큰 정보 조회 (캐시 활용)
            token_info = get_token_info(w3, token_contract)
            token_decimals = token_info["decimals"]

            # 잔고 조회
            contract = w3.eth.contract(
                address=w3.to_checksum_address(token_contract),
                abi=[{
                    "constant": True,
                    "inputs": [{"name": "_owner", "type": "address"}],
                    "name": "balanceOf",
                    "outputs": [{"name": "balance", "type": "uint256"}],
                    "type": "function"
                }]
            )

            raw_balance = contract.functions.balanceOf(w3.to_checksum_address(wallet)).call()
            result["balance"] = round(raw_balance / (10 ** token_decimals), 4)
            balance_success = True
            break

        except Exception as e:
            continue

    # 2. 최근 출금 정보 조회 (Explorer API)
    result["last_withdrawal"] = get_last_withdrawal(chain_name, wallet, token_contract, decimals)

    return result

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

def get_token_market_data(chain_key, contract_address):
    """
    CoinGecko에서 토큰의 시가총액, 24시간 거래량, FDV 등 시장 데이터 조회

    Returns:
        dict: {
            "price": 현재가,
            "market_cap": 시가총액,
            "fdv": 완전희석가치,
            "volume_24h": 24시간 거래량,
            "price_change_24h": 24시간 가격 변동률,
            "circulating_supply": 유통량,
            "total_supply": 총 발행량,
            "source": 데이터 출처
        }
    """
    result = {
        "price": 0,
        "market_cap": 0,
        "fdv": 0,
        "volume_24h": 0,
        "price_change_24h": 0,
        "circulating_supply": 0,
        "total_supply": 0,
        "source": "없음"
    }

    try:
        # CoinGecko token info API (더 상세한 정보)
        url = f"https://api.coingecko.com/api/v3/coins/{chain_key}/contract/{contract_address.lower()}"
        headers = {"Accept": "application/json"}
        res = requests.get(url, headers=headers, timeout=15)

        if res.status_code == 200:
            data = res.json()
            market_data = data.get("market_data", {})

            result["price"] = market_data.get("current_price", {}).get("usd", 0)
            result["market_cap"] = market_data.get("market_cap", {}).get("usd", 0)
            result["fdv"] = market_data.get("fully_diluted_valuation", {}).get("usd", 0)
            result["volume_24h"] = market_data.get("total_volume", {}).get("usd", 0)
            result["price_change_24h"] = market_data.get("price_change_percentage_24h", 0)
            result["circulating_supply"] = market_data.get("circulating_supply", 0)
            result["total_supply"] = market_data.get("total_supply", 0)
            result["source"] = "CoinGecko"

            return result

    except Exception as e:
        pass

    # 상세 API 실패시 기본 price API로 가격만이라도 가져오기
    try:
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{chain_key}"
        params = {
            "contract_addresses": contract_address.lower(),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true"
        }
        headers = {"Accept": "application/json"}
        res = requests.get(url, params=params, headers=headers, timeout=10)

        if res.status_code == 200:
            data = res.json()
            token_data = data.get(contract_address.lower(), {})

            result["price"] = token_data.get("usd", 0)
            result["market_cap"] = token_data.get("usd_market_cap", 0)
            result["volume_24h"] = token_data.get("usd_24h_vol", 0)
            result["price_change_24h"] = token_data.get("usd_24h_change", 0)
            result["source"] = "CoinGecko (Simple)"

    except Exception as e:
        pass

    return result

def format_large_number(num):
    """큰 숫자를 K, M, B 단위로 포맷팅"""
    if num is None or num == 0:
        return "-"
    if num >= 1_000_000_000:
        return f"${num/1_000_000_000:.2f}B"
    elif num >= 1_000_000:
        return f"${num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"${num/1_000:.2f}K"
    else:
        return f"${num:.2f}"

if token_input.startswith("0x") and selected_chain:
    # 토큰 정보 캐시 초기화
    token_info_cache.clear()

    wallets = chain_info[selected_chain]["wallets"]
    explorer = chain_info[selected_chain]["explorer"]
    cg_key = COINGECKO_CHAIN_MAP.get(selected_chain, "ethereum")
    rpc_urls = RPC_URLS.get(selected_chain)
    backup_rpc_url = BACKUP_RPC_URLS.get(selected_chain)

    # API 키 확인
    api_key = get_explorer_api_key(selected_chain)
    if not api_key:
        st.error(f"❌ {selected_chain} Explorer API 키가 설정되지 않았습니다! 최근 출금 정보를 조회할 수 없습니다.")
        st.info("Settings > Secrets에서 [explorer_api_keys] 섹션에 API 키를 추가하세요.")
    else:
        st.success(f"✅ {selected_chain} Explorer API 키 설정됨")

    st.caption("ℹ️ 최근출금: 각 지갑별 최근 입출금 내역 30건 조회 후 업데이트됩니다. (30건 내 출금 없으면 미표시)")

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

    # 토큰 시장 데이터 조회 (가격, 시총, 거래량 등)
    with st.spinner('토큰 시장 데이터 조회 중...'):
        market_data = get_token_market_data(cg_key, token_input)
        token_price = market_data["price"]

        if token_price > 0:
            # 가격 변동률 색상
            price_change = market_data["price_change_24h"]
            if price_change > 0:
                change_color = "green"
                change_icon = "📈"
            elif price_change < 0:
                change_color = "red"
                change_icon = "📉"
            else:
                change_color = "gray"
                change_icon = "➡️"

            # 시장 데이터 표시 (4열)
            st.markdown("### 💹 실시간 시장 데이터")
            mcol1, mcol2, mcol3, mcol4 = st.columns(4)

            with mcol1:
                st.metric(
                    "현재가",
                    f"${token_price:,.6f}" if token_price < 1 else f"${token_price:,.4f}",
                    f"{price_change:+.2f}%" if price_change else None,
                    delta_color="normal"
                )

            with mcol2:
                mc_display = format_large_number(market_data["market_cap"])
                st.metric("시가총액 (MC)", mc_display)

            with mcol3:
                fdv_display = format_large_number(market_data["fdv"])
                st.metric("완전희석가치 (FDV)", fdv_display)

            with mcol4:
                vol_display = format_large_number(market_data["volume_24h"])
                st.metric("24시간 거래량", vol_display)

            # 데이터 출처 표시
            st.caption(f"📊 데이터 출처: {market_data['source']}")
        else:
            # 기존 방식으로 가격만 조회 시도
            token_price, price_source = get_token_price(cg_key, token_input, selected_chain)
            if token_price > 0:
                st.success(f"토큰 가격: ${token_price:,.6f} (출처: {price_source})")
            else:
                st.warning("토큰 가격을 가져올 수 없습니다.")

    # 정렬 옵션
    sort_option = st.radio("정렬 기준", ["잔고 많은 순", "달러 가치 높은 순", "최근 출금 순"], horizontal=True)

    # 순차 처리 모드 (rate limit 방지를 위해 순차 처리)
    # 잔고는 병렬, 출금 정보는 순차로 분리하여 안정성 확보
    with st.spinner(f'잔고 조회 중... (병렬처리: {max_workers}개 워커)'):
        rows = []
        balance_results = {}
        progress_bar = st.progress(0)
        progress_text = st.empty()

        try:
            # 1단계: 잔고만 병렬로 빠르게 조회 (RPC는 rate limit 없음)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_wallet = {}
                for name, addr in wallets.items():
                    future = executor.submit(
                        get_token_balance_rpc,
                        rpc_urls,
                        addr,
                        token_input,
                        name,
                        selected_chain
                    )
                    future_to_wallet[future] = (name, addr)

                completed = 0
                total = len(wallets)

                for future in as_completed(future_to_wallet, timeout=60):
                    try:
                        name, addr = future_to_wallet[future]
                        result = future.result(timeout=10)
                        balance_results[addr] = result.get("balance", 0)
                    except Exception as e:
                        name, addr = future_to_wallet[future]
                        balance_results[addr] = 0

                    completed += 1
                    progress_bar.progress(completed / total)
                    progress_text.text(f"잔고 조회: {completed}/{total}")

        except Exception as e:
            st.error(f"잔고 조회 오류: {str(e)}")

        progress_bar.empty()
        progress_text.empty()

    # 2단계: 출금 정보 순차 조회 (Explorer API rate limit 방지)
    with st.spinner('출금 정보 순차 조회 중... (API 안정성 확보)'):
        progress_bar = st.progress(0)
        progress_text = st.empty()

        completed = 0
        total = len(wallets)
        decimals = token_info.get("decimals", 18) if 'token_info' in dir() else 18

        for name, addr in wallets.items():
            try:
                balance = balance_results.get(addr, 0)
                usd_value = round(balance * token_price, 2) if token_price > 0 else 0
                token_url = f"{explorer}/token/{token_input}?a={addr}"

                # 출금 정보 순차 조회 (rate limit 방지)
                last_wd = get_last_withdrawal(selected_chain, addr, token_input, decimals)

                if last_wd and "error" not in last_wd:
                    # 달러 환산 표시
                    wd_amount_raw = last_wd["amount"]
                    wd_usd = wd_amount_raw * token_price if token_price > 0 else 0
                    wd_amount = f"${format_amount(wd_usd)}" if wd_usd > 0 else format_amount(wd_amount_raw)
                    wd_to = last_wd["to"][:10] + "..." if last_wd["to"] else "-"
                    wd_time = format_time_ago(last_wd["timestamp"])
                    wd_timestamp = last_wd["timestamp"]
                elif last_wd and "error" in last_wd:
                    # 에러 표시 (디버깅용)
                    wd_amount = f"ERR:{last_wd['error'][:15]}"
                    wd_to = "-"
                    wd_time = "-"
                    wd_timestamp = 0
                else:
                    wd_amount = "-"
                    wd_to = "-"
                    wd_time = "-"
                    wd_timestamp = 0

                rows.append({
                    "지갑이름": name,
                    "주소": addr[:10] + "..." + addr[-6:],
                    "잔고": f"{balance:,.4f}",
                    "달러환산": f"${usd_value:,.2f}",
                    "최근출금": f"{wd_amount}",
                    "출금대상": wd_to,
                    "출금시간": wd_time,
                    "출금타임스탬프": wd_timestamp,  # 정렬용
                    "탐색기": token_url,
                    "타입": "CEX"
                })

            except Exception as e:
                rows.append({
                    "지갑이름": name,
                    "주소": addr[:10] + "..." + addr[-6:],
                    "잔고": f"{balance_results.get(addr, 0):,.4f}",
                    "달러환산": f"${balance_results.get(addr, 0) * token_price:,.2f}" if token_price > 0 else "$0.00",
                    "최근출금": "-",
                    "출금대상": "-",
                    "출금시간": "-",
                    "출금타임스탬프": 0,
                    "탐색기": f"{explorer}/token/{token_input}?a={addr}",
                    "타입": "CEX"
                })

            completed += 1
            progress_bar.progress(completed / total)
            progress_text.text(f"출금 정보: {completed}/{total} ({name[:15]}...)")

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
                    "최근출금": "-",
                    "출금대상": "-",
                    "출금시간": "-",
                    "출금타임스탬프": 0,
                    "탐색기": f"{explorer}/address/{liq_info['address']}" if liq_info["address"] else "#",
                    "타입": "DEX"
                })

            if liquidity_infos:
                total_volume = sum(info["volume_24h"] for info in liquidity_infos)
                st.success(f"📊 DEX 24시간 거래량: ${total_volume:,.2f}")
        else:
            st.warning("DEX 유동성 풀을 찾을 수 없습니다.")

    # 데이터프레임 표시
    df = pd.DataFrame(rows)

    # 정렬을 위해 숫자 형태의 컬럼 추가
    df['잔고_숫자'] = df['잔고'].str.replace(',', '').astype(float)
    df['달러_숫자'] = df['달러환산'].str.replace('$', '').str.replace(',', '').astype(float)

    # 정렬
    if sort_option == "잔고 많은 순":
        df = df.sort_values('잔고_숫자', ascending=False)
    elif sort_option == "달러 가치 높은 순":
        df = df.sort_values('달러_숫자', ascending=False)
    else:  # 최근 출금 순
        df = df.sort_values('출금타임스탬프', ascending=False)

    # 정렬/내부용 컬럼 제거
    df = df.drop(['잔고_숫자', '달러_숫자', '출금타임스탬프'], axis=1)

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

    # 테이블 표시 (거의메인 지갑 강조)
    def highlight_main_wallets(row):
        """(거의메인) 지갑을 노란색 배경으로 강조"""
        if "(거의메인)" in str(row.get("지갑이름", "")):
            return ["background-color: #3d3d00; color: #ffff00"] * len(row)
        return [""] * len(row)

    styled_df = df.style.apply(highlight_main_wallets, axis=1)

    st.dataframe(
        styled_df,
        use_container_width=True,
        height=min(len(df) * 40 + 100, 1000),
        column_config={
            "탐색기": st.column_config.LinkColumn(
                "탐색기",
                help="블록 탐색기에서 확인",
                display_text="🔍 확인"
            ),
            "최근출금": st.column_config.TextColumn(
                "최근출금($)",
                help="해당 지갑에서 최근 출금한 금액 (달러 환산)"
            ),
            "출금대상": st.column_config.TextColumn(
                "출금대상",
                help="출금 받은 주소"
            ),
            "출금시간": st.column_config.TextColumn(
                "출금시간",
                help="마지막 출금 시간"
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
        st.write(f"Explorer API 키 설정: {'✅' if api_key else '❌'}")
        if token_info_cache:
            st.write(f"토큰 정보: {token_info_cache.get(token_input, 'N/A')}")

else:
    st.info("정확한 토큰 **컨트랙트 주소**를 입력하세요 (0x로 시작)")

    # API 키 설정 안내
    with st.expander("🔑 Explorer API 키 설정 방법"):
        st.markdown("""
        ### Streamlit Cloud Secrets 설정

        1. Streamlit Cloud 대시보드에서 앱 선택
        2. **Settings** > **Secrets** 메뉴 이동
        3. 아래 형식으로 API 키 추가:

        ```toml
        [explorer_api_keys]
        ETH = "your_etherscan_api_key"
        BSC = "your_bscscan_api_key"
        ARB = "your_arbiscan_api_key"
        OP = "your_optimistic_etherscan_api_key"
        BASE = "your_basescan_api_key"
        AVAX = "your_snowtrace_api_key"
        POL = "your_polygonscan_api_key"
        ```

        ### API 키 발급 링크
        - [Etherscan](https://etherscan.io/apis)
        - [BscScan](https://bscscan.com/apis)
        - [Arbiscan](https://arbiscan.io/apis)
        - [Optimistic Etherscan](https://optimistic.etherscan.io/apis)
        - [BaseScan](https://basescan.org/apis)
        - [Snowtrace](https://snowtrace.io/apis)
        - [PolygonScan](https://polygonscan.com/apis)
        """)

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




