import io
import os
import time
from typing import Any, Dict, List, Type
import zipfile
import aiohttp
import pandas as pd
import requests
from ..providers.IProvider import IDataProvider
from ..enums.Enums import ProviderName
from ..processing.Mappers import IColumnMapper
from ..processing.Selection import SearchCriteria
from ..core.ErrorHandle import NetworkError, ProviderError
from ..processing.ResultHandle import async_result_decorator, result_decorator


class AFADDataProvider(IDataProvider):
    """AFAD veri sağlayıcı"""

    def __init__(self, column_mapper: Type[IColumnMapper], timeout: int = 30):
        self.timeout = timeout
        self.column_mapper = column_mapper
        self.name = ProviderName.AFAD.value
        self.base_url = "https://ivmeservis.afad.gov.tr/Waveforms/GetWaveforms"
        self.base_download_dir = "Afad_events"
        self.mapped_df = None
        self.response_df = None
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """Genel arama kriterlerini provider'a özel formata dönüştür"""
        return criteria.to_afad_params()

    @async_result_decorator
    async def fetch_data_async(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """AFAD verilerini getir"""
        try:
            payload = criteria
            print(f"AFAD arama kriterleri: {payload}")

            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.post(
                    self.base_url,
                    json=payload,
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json() #AFAD API'si JSON formatında veri döndürüyor
                        self.response_df = pd.DataFrame(data) #JSON verisini DataFrame'e dönüştür
                        self.mapped_df = self.column_mapper.map_columns(df=self.response_df) #Verileri standart kolonlara eşleştir
                        self.mapped_df['PROVIDER'] = str(self.name) #Sağlayıcı adını ekle
                        print(f"AFAD'dan {len(self.mapped_df)} kayıt alındı.")
                        self.mapped_df['ENDPOINTSOURCE'] = "https://tadas.afad.gov.tr/waveform-detail/" + self.mapped_df['RSN'].astype(str) #Kayıtlara AFAD detay sayfası linki ekle
                        return self.mapped_df
                    else:
                        error_text = await response.text()
                        raise NetworkError(
                            self.name,
                            Exception(f"HTTP {response.status}: {error_text}"),
                            "AFAD API request failed"
                        )
        except aiohttp.ClientError as e:
            raise NetworkError(self.name, e, "AFAD network error")
        except Exception as e:
            raise ProviderError(self.name, e, f"AFAD data processing failed: {e}")

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """AFAD verilerini getir (senkron)"""
        try:
            response = self._search_afad(criteria=criteria,
                                         headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                self.response_df = pd.DataFrame(data)
                self.mapped_df = self.column_mapper.map_columns(df=self.response_df)
                self.mapped_df['PROVIDER'] = str(self.name)
                print(f"AFAD'dan {len(self.mapped_df)} kayıt alındı.")
                return self.mapped_df
            else:
                raise NetworkError(
                    self.name,
                    Exception(f"HTTP {response.status_code}: {response.text}"),
                    "AFAD API request failed"
                )
        except requests.RequestException as e:
            raise NetworkError(self.name, e, "AFAD network error")
        except Exception as e:
            raise ProviderError(self.name, e, f"AFAD data processing failed: {e}")

    def _search_afad(self,
                     criteria: Dict[str, Any],
                     headers: dict) -> requests.Response:
        """AFAD API'sini kullanarak arama yap"""
        payload = criteria
        print(f"AFAD arama kriterleri: {payload}")
                
        response = requests.post(
            self.base_url,
            json=payload,
            headers=headers,
            timeout=self.timeout
        )
        
        return response

    def get_name(self) -> str:
        return str(self.name)

    @result_decorator
    def get_event_details(self, event_ids: List[int]) -> pd.DataFrame:
        """Birden fazla event için detaylı bilgileri alır"""
        all_details = []
        
        for event_id in event_ids:
            detail_url = f"https://ivmeservis.afad.gov.tr/Event/GetEventById/{event_id}"
            
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://tadas.afad.gov.tr',
                'Referer': f'https://tadas.afad.gov.tr/event-detail/{event_id}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Username': 'GuestUser',
                'IsGuest': 'true'
            }
            
            try:
                response = requests.get(url=detail_url, headers=headers, timeout=30)
                if response.status_code == 200:
                    detail_data = response.json()
                    if isinstance(detail_data, dict):
                        all_details.append(detail_data)
                    elif isinstance(detail_data, list) and len(detail_data) > 0:
                        all_details.append(detail_data[0])
                
                time.sleep(0.1)
                
            except Exception as e:
                raise ProviderError(self.name, e, f"Event {event_id} details failed")
        
        return pd.DataFrame(all_details) if all_details else pd.DataFrame()

    def _waveform_folder_route(self, event_id: int) -> str:
        """AFAD dalga formu dosyalarının kaydedileceği klasör yapısını oluşturur"""
        event_dir = os.path.join(self.base_download_dir, f"event_{event_id}")
        os.makedirs(event_dir, exist_ok=True)
        return event_dir
    
    def save_waveform_zipfile(self, zip_content: bytes, event_id: int, station_id: str) -> str:
        """AFAD'dan indirilen zip dosyasını kaydeder geriye dosya yolunu döndürür
        Args:
            zip_content (bytes): AFAD API'sinden gelen zip dosyasının içeriği
            event_id (int): İlgili deprem olayının ID'si (klasör yapısı için)
            station_id (str): İstasyon kodu (dosya adlandırması için)
        """
        folder_dir = self._waveform_folder_route(event_id=event_id)
        zip_path = os.path.join(folder_dir, f"waveforms_{event_id}_{station_id}.zip")
        with open(zip_path, 'wb') as f:
            f.write(zip_content)
        return zip_path

    def extract_and_organize_zip(self, zip_path: str, export_type: str) -> List[str]:
        """Zip dosyasını açar ve içindeki dosyaları organize eder"""
        extracted_files = []
        MIN_ZIP_SIZE = 1024  # bytes, treat much smaller files as suspicious

        # Quick size sanity check
        try:
            size = os.path.getsize(zip_path)
            if size < MIN_ZIP_SIZE:
                raise ProviderError(self.name, None, f"[WARNING] İndirilen zip dosyası çok küçük ({size} bytes): {zip_path}")
        except OSError:
            # If we can't stat the file, let the normal zip handling surface the error
            pass

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Test for first bad file inside the zip (returns name of bad file or None)
                bad_file = zip_ref.testzip()
                if bad_file is not None:
                    raise ProviderError(self.name, None, f"[ERROR] Hasarlı zip içeriği: {bad_file} in {zip_path}")

                # Extract all top-level files into the same folder as the zip
                target_dir = os.path.dirname(zip_path)
                zip_ref.extractall(target_dir)

                for file_info in zip_ref.infolist():
                    member_name = file_info.filename
                    abs_path = os.path.join(target_dir, member_name)

                    # If the member is itself a zip and ascii export requested, extract nested zip
                    if member_name.endswith('.zip') and export_type in ("asc", "asc2"):
                        try:
                            # Read inner zip bytes and open with BytesIO for safety
                            inner_bytes = zip_ref.read(member_name)
                            with zipfile.ZipFile(io.BytesIO(inner_bytes), 'r') as inner_zip:
                                inner_zip.extractall(target_dir)
                                extracted_files.extend([os.path.join(target_dir, f) for f in inner_zip.namelist()])
                        except zipfile.BadZipFile:
                            raise ProviderError(self.name, None, f"[ERROR] İç zip hasarlı: {member_name} inside {zip_path}")
                        except Exception as e:
                            # don't stop processing other files for a single nested failure
                            print(f"[ERROR] İç zip çıkarma hatası: {member_name} -> {e}")
                            continue
                    else:
                        extracted_files.append(abs_path)

        except zipfile.BadZipFile:
            raise ProviderError(self.name, None, f"[ERROR] Hasarlı zip dosyası: {zip_path}")
        except ProviderError:
            # re-raise ProviderError unchanged
            raise
        except Exception as e:
            raise ProviderError(self.name, e, f"Zip extraction failed: {e}")

        # Optionally remove the original zip if extraction succeeded
        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)
        except Exception:
            pass

        return extracted_files

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs) -> bool|ProviderError:
        """
        Downloads AFAD waveform files in batches, saves them as zip files, and extracts the contents.
        Args:
            filename (str): The filename to download. "self.mapped_df içindeki FILE_NAME_H1, FILE_NAME_H2, FILE_NAME_V kolonlarında bulunan dosya adları üzerinden indirme yapılacak."
            file_type (str, optional): Type of file to download. Defaults to 'unprocessed'.
            file_status (str, optional): Status of the file. Defaults to 'RawAcc'. Options --> "RawAcc", "Acc", "Vel", "Disp", "ResSpecAcc", "ResSpecVel", "ResSpecDisp", "FFT", "Husid"
            export_type (str, optional): Export format for the files. Defaults to 'asc2'. Options --> asc2, mseed, asd
            user_name (str, optional): Name of the user requesting the download. Defaults to 'GuestUser'.
            event_id (str or int, optional): Event ID for organizing downloaded files. If not provided, a timestamp is used.
        Returns:
            Dict: A dictionary containing the result of the download operation, including success status and message.
        Raises:
            ProviderError: If any error occurs during the download or extraction process.
        """
        file_type   = kwargs.get('file_type', 'unprocessed')
        file_status = kwargs.get('file_status', 'RawAcc')
        export_type = kwargs.get('export_type', 'asc2')
        user_name   = kwargs.get('user_name', 'GuestUser')
        event_id    = kwargs.get('event_id', int(time.time())) # Event ID yoksa timestamp kullan
        station_id  = kwargs.get('station_code',filename.split('_')[-1] if '_' in filename else "unknown_station")

        url = "https://ivmeprocessguest.afad.gov.tr/ExportData"

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }
        payload = {
                "filename": [filename],
                "file_type": [file_type],
                "file_status": file_status,
                "export_type": export_type,
                "user_name": user_name,
                "call": "afad"
            }

        try:
            # POST isteği gönder
            response = requests.post(url, headers=headers, json=payload, timeout=50)
            response.raise_for_status()
            zip_path = self.save_waveform_zipfile(zip_content=response.content, event_id=event_id, station_id=station_id)
            extr_files = self.extract_and_organize_zip(zip_path=zip_path, export_type=export_type)
            return True
        
                
        except requests.RequestException as e:
            raise ProviderError(provider_name=self.name, original_error=e, message=f"Waveform download failed: {e}")
        
    
    @result_decorator
    def download_afad_waveforms_batch(self,
                                      filenames: List[str], **kwargs) -> Dict:
        """
        Downloads AFAD waveform files in batches, saves them as zip files, and extracts the contents.
        Args:
            filenames (List[str]): List of filenames to download.
            file_type (str, optional): Type of file to download. Defaults to 'ap'.
            file_status (str, optional): Status of the file. Defaults to 'Acc'. Options --> "RawAcc", "Acc", "Vel", "Disp", "ResSpecAcc", "ResSpecVel", "ResSpecDisp", "FFT", "Husid"
            export_type (str, optional): Export format for the files. Defaults to 'mseed'. Options --> asc2, mseed, asd
            user_name (str, optional): Name of the user requesting the download. Defaults to 'GuestUser'.
            event_id (str or int, optional): Event ID for organizing downloaded files. If not provided, a timestamp is used.
            batch_size (int, optional): Number of files per batch. Defaults to 10, maximum allowed is 10.
        Returns:
            Dict: A dictionary containing download statistics and batch results, including:
                - total_files: Total number of files requested.
                - batches: List of batch result dictionaries.
                - successful_batches: Number of batches downloaded successfully.
                - failed_batches: Number of batches that failed to download.
                - downloaded_files: Total number of files downloaded and extracted.
        Raises:
            ProviderError: If any error occurs during the download or extraction process.
        """

        file_type   = kwargs.get('file_type', 'ap')
        file_status = kwargs.get('file_status', 'Acc')
        export_type = kwargs.get('export_type', 'mseed')
        user_name   = kwargs.get('user_name', 'GuestUser')
        event_id    = kwargs.get('event_id')
        batch_size  = kwargs.get('batch_size', 10)

        batch_size = min(batch_size, 10) # Batch size'ı maximum 10 ile sınırla

        url = "https://ivmeprocessguest.afad.gov.tr/ExportData"

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }
        
        all_results = {
            'total_files': len(filenames),
            'batches': [],
            'successful_batches': 0,
            'failed_batches': 0,
            'downloaded_files': 0
        }
        
        # Dosyaları batch'lere ayır
        batches = [filenames[i:i + batch_size] for i in range(0, len(filenames), batch_size)]
        
        print(f"[INFO] {len(filenames)} dosya, {len(batches)} parti halinde indirilecek (max {batch_size}/parti)")
        for batch_index, batch_filenames in enumerate(batches, 1):
            print(f"[INFO] PARTİ {batch_index}/{len(batches)} - {len(batch_filenames)} dosya")
            
            # Request payload
            payload = {
                "filename": batch_filenames,
                "file_type": [file_type] * len(batch_filenames),
                "file_status": file_status,
                "export_type": export_type,
                "user_name": user_name,
                "call": "afad"
            }
            try:
                # POST isteği gönder
                    response = requests.post(url, headers=headers, json=payload, timeout=50)
                    response.raise_for_status()
                    
                    # Event ID'yi kullanarak klasör yapısı oluştur
                    if event_id:
                        event_dir = os.path.join(self.base_download_dir, str(event_id))
                    else:
                        # Event ID yoksa timestamp kullan
                        event_dir = os.path.join(self.base_download_dir, f"event_{int(time.time())}")
                    
                    # # Batch klasörü oluştur
                    # batch_dir = os.path.join(event_dir, f"batch_{batch_index}")
                    os.makedirs(event_dir, exist_ok=True)
                    
                    # Zip dosyasını kaydet
                    zip_filename = f"part_{batch_index}.zip"
                    zip_path = os.path.join(event_dir, zip_filename)
                    
                    with open(zip_path, 'wb') as f:
                        f.write(response.content) # Zip dosyasını kaydet
                    
                    # Zip dosyasını aç ve organize et
                    extracted_files = self.extract_and_organize_zip_batch(event_path=event_dir, zip_path=zip_path, expected_filenames=batch_filenames,export_type=export_type)
                    
                    batch_result = {
                        'batch_number': batch_index,
                        'filenames': batch_filenames,
                        'batch_size': len(batch_filenames),
                        'zip_file': zip_path,
                        'extracted_files': extracted_files,
                        'extracted_count': len(extracted_files),
                        'success': True,
                        'error': None
                    }

                    # Başarısız dosyaları kontrol et ve yeniden dene
                    if len(extracted_files) < len(batch_filenames):
                        failed_files = [f for f in batch_filenames if f not in [os.path.basename(x) for x in extracted_files]]
                        if failed_files:
                            print(f"[ERROR]  {len(failed_files)} dosya çıkarılamadı, yeniden deneniyor...")
                            successful_retries = self.retry_failed_downloads(
                                event_id=event_id,
                                failed_filenames=failed_files,
                                export_type='mseed',
                                file_status=file_status
                            )
                            extracted_files.extend(successful_retries)
                    
                    all_results['batches'].append(batch_result)
                    all_results['successful_batches'] += 1
                    all_results['downloaded_files'] += len(extracted_files)
                    
                    print(f"[OK] Parti {batch_index} başarılı: {len(extracted_files)} dosya")
                    
                    # Partiler arasında bekle (sunucu yükünü azaltmak için)
                    if batch_index < len(batches):
                        wait_time = 10
                        print(f"[INFO]{wait_time} saniye bekleniyor...")
                        time.sleep(wait_time)
                    
            except Exception as e:
                raise ProviderError(self.name, e, f"Waveform download failed: {e}")

            return all_results

    def extract_and_organize_zip_batch(self,
                                   event_path: str,
                                   zip_path: str,
                                   expected_filenames: List[str],
                                   export_type: str) -> List[str]:
        """
        Zip dosyasını aç ve dosyaları organize et (batch versiyonu)
        - Hasarlı dosyaları tespit et ve yeniden dene
        - ASCII formatında iç içe zip'leri çıkar
        - MSEED formatını düzgün işle
        """
        extracted_files = []
        
        try:
            # Önce zip dosyasının geçerli olup olmadığını kontrol et
            try:
                with zipfile.ZipFile(zip_path, 'r') as test_zip:
                    test_zip.testzip()  # Hasarlı dosyaları kontrol et
            except zipfile.BadZipFile:
                print(f"[ERROR] Hasarlı zip dosyası: {zip_path}")
                # Hasarlı dosyayı sil ve None döndür (yeniden deneme için)
                try:
                    os.remove(zip_path)
                except:
                    pass
                return []

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Zip içindeki tüm dosyaları listele
                zip_files = zip_ref.namelist()
                
                for filename in zip_files:
                    try:
                        # Dosya adından station ID'yi çıkar
                        if '_' in filename:
                            base_name = os.path.splitext(filename)[0]
                            parts = base_name.split('_')
                            
                            if len(parts) >= 2:
                                # Station ID'yi al (genellikle son parça)
                                station_id = parts[-1]
                                
                                # Hedef klasörü oluştur
                                target_dir = os.path.join(event_path, f"{station_id}")
                                os.makedirs(target_dir, exist_ok=True)
                                
                                target_path = os.path.join(target_dir, filename)
                                
                                # Dosyayı çıkar
                                with open(target_path, 'wb') as f:
                                    f.write(zip_ref.read(filename))
                                
                                # Eğer çıkarılan dosya bir zip ise, içindekileri de çıkar
                                if filename.endswith('.zip') and export_type in ["asc","asc2"]:
                                    nested_zip_path = target_path
                                    nested_extracted = self.extract_nested_zip(nested_zip_path, target_dir)
                                    extracted_files.extend(nested_extracted)
                                    
                                    # İç zip dosyasını temizle (opsiyonel)
                                    # try:
                                    #     os.remove(nested_zip_path)
                                    # except:
                                    #     pass
                                else:
                                    extracted_files.append(target_path)
                                    
                    except Exception as e:
                        print(f"[ERROR] {filename} işlenirken hata: {e}")
                        continue
            
            # Başarılı çıkarma sonrası zip'i temizle
            try:
                os.remove(zip_path)
            except:
                pass
                
        except zipfile.BadZipFile:
            print(f"[ERROR] Hasarlı zip dosyası: {zip_path}")
            try:
                os.remove(zip_path)
            except:
                pass
            return []
        except Exception as e:
            print(f"[ERROR] Zip açma hatası: {e}")
        
        return extracted_files

    def retry_failed_downloads(self, event_id: int,
                               failed_filenames: List[str],
                               export_type: str,
                               file_status: str,
                               max_retries: int = 3) -> List[str]:
        """
        Başarısız indirmeleri yeniden dene
        """
        successful_downloads = []
        
        for retry in range(max_retries):
            if not failed_filenames:
                break
                
            print(f"🔄 {len(failed_filenames)} dosya için {retry + 1}. yeniden deneme...")
            
            # 10'arli gruplar halinde yeniden dene
            batches = [failed_filenames[i:i + 10] for i in range(0, len(failed_filenames), 10)]
            
            for batch in batches:
                try:
                    result = self.download_afad_waveforms_batch(
                        event_id=event_id,
                        filenames=batch,
                        export_type=export_type,
                        file_status=file_status
                    )
                    
                    # Başarılı indirmeleri listeden çıkar
                    if result and 'batches' in result:
                        for batch_result in result['batches']:
                            if batch_result.get('success', False):
                                successful_downloads.extend(batch_result.get('filenames', []))
                                # Başarılı dosyaları failed listesinden çıkar
                                failed_filenames = [f for f in failed_filenames if f not in batch_result.get('filenames', [])]
                    
                    # Yeniden denemeler arasında bekle
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"[ERROR] Yeniden deneme hatası: {e}")
            
            if not failed_filenames:
                break
                
            # Sonraki deneme öncesi bekle
            time.sleep(2)
        
        return successful_downloads

    def extract_nested_zip(self, zip_path: str, target_dir: str) -> List[str]:
        """
        İç içe zip dosyalarını çıkar
        """
        extracted_files = []

        try:
            with zipfile.ZipFile(zip_path, 'r') as nested_zip:
                nested_files = nested_zip.namelist()

                for nested_file in nested_files:
                    try:
                        nested_target_path = os.path.join(target_dir,
                                                          nested_file)

                        # İç zip'teki dosyayı çıkar
                        with open(nested_target_path, 'wb') as f:
                            f.write(nested_zip.read(nested_file))

                        extracted_files.append(nested_target_path)

                    except Exception as e:
                        print(f"[ERROR] İç zip dosyası {nested_file} işlenirken hata: {e}")
                        continue

        except Exception as e:
            print(f"[ERROR] İç zip açma hatası: {e}")

        return extracted_files
