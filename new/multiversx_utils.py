import requests 
import time
from tqdm import tqdm
from fake_useragent import UserAgent
from multiprocessing import Pool
import base64
from selenium import webdriver
from selenium_stealth import stealth
from selenium.webdriver.common.by import By

def clear_keys(dictionary):
    keys = {}
    for nft in dictionary.values():
        for key in nft.keys():
            if key not in keys:
                keys[key] = 0
            keys[key] += 1
    dictionary_length = len(dictionary)
    key_to_remove = [key for key, count in keys.items() if count != dictionary_length]
    for identifier in dictionary:
        for key in key_to_remove:
            if key in dictionary[identifier]:
                del dictionary[identifier][key]
    return dictionary

def get_total_nfts(collection_name, sleep_time=0.4):
    user_agent = UserAgent()
    while True:
        try:
            url = f'https://api.elrond.com/collections/{collection_name}/nfts/count'
            headers = {'User-Agent': user_agent.random}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                total_nfts = response.json()
                break
            elif response.status_code == 404:
                return None
            else:
                time.sleep(sleep_time)
        except:
            time.sleep(sleep_time)
    return total_nfts

def get_collection_info(collection_name, sleep_time=0.4):
    ua = UserAgent()
    while True:
        try:
            url = f'https://api.elrond.com/collections/{collection_name}'
            headers = {'User-Agent': ua.random}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                collection_info = response.json()
                break
            if response.status_code == 404:
                return None
            else:
                time.sleep(sleep_time)
        except:
            time.sleep(sleep_time)
    collection_info['totalNfts'] = get_total_nfts(collection_name)
    return collection_info

def get_nft(identifier, sleep_time=0.4):
    user_agent = UserAgent()
    while True:
        try:
            url = f'https://api.elrond.com/nfts/{identifier}'
            headers = {'User-Agent': user_agent.random}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                nft = response.json()
                break
            elif response.status_code == 404:
                return None
            else:
                time.sleep(sleep_time)
        except:
            time.sleep(sleep_time)
    return nft

def get_collection_nfts(collection_name, sleep_time=0.4):
    collection_info = get_collection_info(collection_name)
    if collection_info is None:
        return None
    user_agent = UserAgent()
    length = 0
    collection_nfts = {}
    bar = tqdm(total=collection_info['totalNfts'], desc=f"get_collection_nfts('{collection_name}')", position=0)
    for order in ['asc', 'desc']:
        start = 0
        stop = 10000
        step = 100
        for index in range(start, stop, step):
            while True:
                url = f'https://api.elrond.com/collections/{collection_name}/nfts?from={index}&size={step}&withOwner=true&sort=nonce&order={order}'
                headers = {'User-Agent': user_agent.random}
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    for nft in response.json():
                        if 'owner' not in nft:
                            nft = get_nft(nft['identifier'], sleep_time)
                        collection_nfts[nft['identifier']] = nft
                    new_length = len(collection_nfts)
                    bar.update(new_length - length)
                    if new_length == length:
                        collection_nfts = clear_keys(collection_nfts)
                        collection_nfts = dict(sorted(collection_nfts.items()))
                        return collection_nfts
                    length = new_length
                    break
                if response.status_code == 429:
                    time.sleep(sleep_time)
    collection_nfts = clear_keys(collection_nfts)
    collection_nfts = dict(sorted(collection_nfts.items()))
    return collection_nfts

def get_collection_nfts_worker(args):
    start, stop, collection_name, sleep_time = args
    sub_collection_nfts = {}
    for index in range(start, stop):
        if len(hex(index)) % 2 == 1:
            hex_index = f'0{hex(index)[2:]}'
        else:
            hex_index = hex(index)[2:]
        identifier = f'{collection_name}-{hex_index}'
        nft = get_nft(identifier, sleep_time)
        if nft is not None:
            sub_collection_nfts[identifier] = nft
    return sub_collection_nfts

def get_collection_nfts_master(collection_name, sleep_time=0.4, core=4):
    collection_info = get_collection_info(collection_name)
    if collection_info is None:
        return None
    bar = tqdm(total=collection_info['totalNfts'], desc=f"get_collection_nfts('{collection_name}')", position=0)
    collection_nfts = {}
    start = 0
    stop = collection_info['totalNfts']
    step = 20
    while len(collection_nfts) != collection_info['totalNfts']:
        chunks = [(i, min(i+step, stop), collection_name, sleep_time) for i in range(start, stop, step)]
        with Pool(core) as p:
            for sub_collection_nfts in p.imap(get_collection_nfts_worker, chunks):
                collection_nfts.update(sub_collection_nfts)
                bar.update(len(sub_collection_nfts))
                if len(collection_nfts) == collection_info['totalNfts']:
                    break
        new_stop = stop + max(step*core, stop//2)
        start = stop
        stop = new_stop
    bar.close()
    collection_nfts = clear_keys(collection_nfts)
    collection_nfts = dict(sorted(collection_nfts.items()))
    return collection_nfts

def get_collection_nfts_slow(collection_name, sleep_time=0.4, core=1):
    if core != 1:
        return get_collection_nfts_master(collection_name, sleep_time, core)
    collection_info = get_collection_info(collection_name)
    if collection_info is None:
        return None
    bar = tqdm(total=collection_info['totalNfts'], desc=f"get_collection_nfts('{collection_name}')", position=0)
    collection_nfts = {}
    for index in range(0, collection_info['totalNfts']):
        if len(hex(index)) % 2 == 1:
            hex_index = f'0{hex(index)[2:]}'
        else:
            hex_index = hex(index)[2:]
        identifier = f'{collection_name}-{hex_index}'
        nft = get_nft(identifier, sleep_time)
        if nft is not None:
            collection_nfts[identifier] = nft
            bar.update(1)
    bar.close()
    collection_nfts = clear_keys(collection_nfts)
    collection_nfts = dict(sorted(collection_nfts.items()))
    return collection_nfts

def open_stealth_driver(headless=False, maximize=True, options=None):
    if options is None:
        options = webdriver.ChromeOptions()
        if maximize:
            options.add_argument("start-maximized")
        if headless:
            options.add_argument("--headless")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
    return driver

def get_nft_price(stealth_driver, identifier, sleep_time=0.4):
    currency_path = '/html/body/div[1]/div[1]/main/div[2]/div[2]/div[3]/div[1]/div[2]/div[1]/div[1]/div/div/div/img'
    amount_path = '/html/body/div[1]/div[1]/main/div[2]/div[2]/div[3]/div[1]/div[2]/div[1]/div[1]/div/div/div/span'
    try:
        url = f'https://xoxno.com/nft/{identifier}'
        stealth_driver.get(url)
        time.sleep(sleep_time)
        currency = stealth_driver.find_element(By.XPATH, currency_path).get_attribute('src').split('/')[-1].split('.')[0].upper()
        amount = stealth_driver.find_element(By.XPATH, amount_path).text
        amount = float(amount.replace('K', ''))*1000 if 'K' in amount else float(amount)
        return {
            'currency': currency,
            'amount': amount
        }
    except:
        pass
    return {
        'currency': None,
        'amount': None
    }

def get_collection_offchain_data(collection_nfts, sleep_time=0.4, whitelist=None, blacklist=['https://ipfs.io/ipfs/', 'https://gateway.pinata.cloud/ipfs/']):
    xoxno_address = 'erd1qqqqqqqqqqqqqpgq6wegs2xkypfpync8mn2sa5cmpqjlvrhwz5nqgepyg8'
    user_agent = UserAgent()
    stealth_driver = open_stealth_driver(headless=True)
    collection_offchain_data = {}
    if len(collection_nfts) == 0:
        return collection_offchain_data
    collection_name = '-'.join(list(collection_nfts.keys())[0].split('-')[:-1])
    bar = tqdm(total=len(collection_nfts), desc=f"get_collection_offchain_data('{collection_name}')", position=0)
    for identifier in collection_nfts:
        while True:
            try:
                offchain_data = {}
                for uri in collection_nfts[identifier]['uris']:
                    url = base64.b64decode(uri).decode('utf-8')
                    if blacklist is not None and any([url.startswith(b) for b in blacklist]):
                        offchain_data[url] = None
                    elif whitelist is not None and not any([url.startswith(w) for w in whitelist]):
                        offchain_data[url] = None
                    else:
                        while True:
                            time.sleep(sleep_time)
                            headers = {'User-Agent': user_agent.random}
                            response = requests.get(url, headers=headers)
                            if response.status_code == 200:
                                offchain_data[url] = response.json()
                                break
                            else:
                                time.sleep(sleep_time*100)
                if collection_nfts[identifier]['owner'] != xoxno_address:
                    offchain_data['price'] = {
                        'currency': None,
                        'amount': None
                    }
                else:
                    offchain_data['price'] = get_nft_price(stealth_driver, identifier)
                collection_offchain_data[identifier] = offchain_data
                bar.update(1)
                break
            except:
                pass
    stealth_driver.close()
    bar.close()
    return collection_offchain_data

def parse_nft_data(nft):
    def get_realPriceAmount(priceAmount, starLevel):
        if priceAmount is None:
            return priceAmount
        return priceAmount / (max(1, 3*(starLevel-1)))
    # onchain data
    onchain_data = {}
    onchain_data['identifier'] = nft.get('identifier', None)
    onchain_data['collection'] = nft.get('collection', None)
    onchain_data['name'] = nft.get('name', None)
    onchain_data['url'] = nft['media'][-1].get('url', None) if nft.get('media', []) != [] else None
    onchain_data['thumbnailUrl'] = nft['media'][-1].get('thumbnailUrl', None) if nft.get('media', []) != [] else None
    onchain_data['owner'] = nft.get('owner', None)
    onchain_data['rank'] = nft.get('rank', None)
    if nft['metadata'] != {}:
        for attribute in nft['metadata'].get('attributes', []):
            onchain_data[attribute['trait_type']] = attribute['value']
    if onchain_data['collection'] in ['EAPES-8f3c1f']:
        for attibute in ['Background', 'Type', 'Eyes', 'Mouth', 'Clothes', 'Earring', 'priceCurrency', 'priceAmount', 'Hat', 'Special']:
            onchain_data[attibute] = 'None'
    # offchain data
    offchain_data = {}
    offchain_data['priceCurrency'] = nft['offchainData']['price']['currency']
    offchain_data['priceAmount'] = nft['offchainData']['price']['amount']
    if onchain_data['collection'] in ['CRMYTH-546419', 'CRWEAPONS-e5ab49']:
        for url, data in nft['offchainData'].items():
            if url.startswith('https://metadata.cantinaroyale.io/dynamic/'):
                if 'error' in data:
                    for attibute in ['xp', 'wear', 'level', 'starLevel', 'Damage', 'Reload Time', 'Ammo', 'Range']:
                        offchain_data[attibute] = None
                else:
                    for key, value in data.items():
                        if key == 'stats':
                            for attribute in value:
                                offchain_data[attribute['name']] = attribute['value']
                        else:
                            offchain_data[key] = value
        offchain_data['realPriceAmount'] = get_realPriceAmount(nft['offchainData']['price']['amount'], offchain_data['starLevel'])
    elif onchain_data['collection'] in ['CRCHAMPS-d0265d', 'CRHEROES-9edff2']:
        for url, data in nft['offchainData'].items():
            if url.startswith('https://metadata.cantinaroyale.io/metadata/'):
                if 'error' in data:
                    for attibute in ['Background', 'Body', 'Earrings', 'Eyes', 'Face', 'Head', 'Headgear', 'LegAccessories', 'Legs', 'Mouth', 'Perk 1', 'Perk 2', 'Rarity Class', 'Skin', 'Species']:
                        offchain_data[attibute] = None
                else:
                    for attribute in data['attributes']:
                        offchain_data[attribute['trait_type']] = attribute['value']
            elif url.startswith('https://metadata.cantinaroyale.io/dynamic/'):
                if 'error' in data:
                    for attibute in ['level', 'health', 'shield', 'talent_points_available', 'talent_points_total', 'earn_rate', 'character_tokens', 'Overachiever', 'Hodler', 'Grounded', 'Stonewall', 'Adrenaline Rush', 'Overshield', 'Black Widow', 'Galvanized', 'Nano Meds', 'Resilience', 'Cold Blooded', 'Perseverance', 'Escape Artist', 'Scavenger', 'Cool Moves', 'Brawler', 'Automatic Weapons Proficiency', 'Scatter Weapons Proficiency', 'Precision Weapons Proficiency', 'Explosive Weapons Proficiency', 'Elemental Weapons Proficiency']:
                        offchain_data[attibute] = None
                else:
                    for key, value in data['gameData'][-1]['dynamicData'].items():
                        if key == 'talents':
                            for attribute in value:
                                offchain_data[attribute['name']] = attribute['value']
                        else:
                            offchain_data[key] = value
    elif onchain_data['collection'] in ['GSPACEAPE-08bc2b', 'CEA-2d29f9']:
        for url, data in nft['offchainData'].items():
            if url.startswith('https://metadata.verko.io/dynamic/'):
                if 'error' in data:
                    for attibute in ['level', 'health', 'shield', 'talent_points_available', 'talent_points_total', 'earn_rate', 'character_tokens', 'Overachiever', 'Hodler', 'Grounded', 'Stonewall', 'Adrenaline Rush', 'Overshield', 'Black Widow', 'Galvanized', 'Nano Meds', 'Resilience', 'Cold Blooded', 'Perseverance', 'Escape Artist', 'Scavenger', 'Cool Moves', 'Brawler', 'Automatic Weapons Proficiency', 'Scatter Weapons Proficiency', 'Precision Weapons Proficiency', 'Explosive Weapons Proficiency', 'Elemental Weapons Proficiency']:
                        offchain_data[attibute] = None
                else:
                    for key, value in data['gameData'][-1]['dynamicData'].items():
                        if key == 'talents':
                            for attribute in value:
                                offchain_data[attribute['name']] = attribute['value']
                        else:
                            offchain_data[key] = value
    nft_data = {**onchain_data, **offchain_data}
    new_nft_data = {}
    for key, value in nft_data.items():
        new_key = key.replace('_', ' ').split(' ')
        new_key = ''.join([word[0].upper()+word[1:] for word in new_key])
        new_key = new_key[0].lower() + new_key[1:]
        new_nft_data[new_key] = value if value != "None" else None
    return new_nft_data