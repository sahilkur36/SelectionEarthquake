import hashlib
import os
import time
import pandas as pd
from typing import Optional

class CacheManager:
    def __init__(self, cache_dir: str = ".cache", expiry_hours: int = 24):
        self.cache_dir = cache_dir
        self.expiry_seconds = expiry_hours * 3600
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def _generate_key(self, provider_name: str, criteria: any) -> str:
        # Pydantic SearchCriteria kullanıyorsan model_dump_json() en güvenli yoldur
        criteria_json = criteria.model_dump_json()
        raw_key = f"{provider_name}_{criteria_json}"
        return hashlib.md5(raw_key.encode()).hexdigest()

    def get(self, provider_name: str, criteria: any) -> Optional[pd.DataFrame]:
        key = self._generate_key(provider_name, criteria)
        file_path = os.path.join(self.cache_dir, f"{key}.parquet")
        
        if not os.path.exists(file_path):
            return None

        # --- ZAMAN AŞIMI KONTROLÜ ---
        file_mod_time = os.path.getmtime(file_path) # Dosyanın son değiştirilme zamanı
        if (time.time() - file_mod_time) > self.expiry_seconds:
            print(f"[CACHE] {provider_name} verisi çok eski (expired). Siliniyor...")
            os.remove(file_path)
            return None
        # ----------------------------

        try:
            return pd.read_parquet(file_path, engine='pyarrow')
        except Exception as e:
            print(f"[CACHE ERROR] Okuma hatası: {e}")
            return None

    def set(self, provider_name: str, criteria: any, df: pd.DataFrame):
        if df is None or df.empty:
            return
            
        key = self._generate_key(provider_name, criteria)
        file_path = os.path.join(self.cache_dir, f"{key}.parquet")
        
        try:
            # engine='pyarrow' için pip install pyarrow gerekli
            df.to_parquet(file_path, engine='pyarrow', index=False)
        except Exception as e:
            print(f"[CACHE ERROR] Yazma hatası: {e}")