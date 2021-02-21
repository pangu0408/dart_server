import requests
import json
# from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import datetime
import numpy as np
import os
import psycopg2 as pg2 
import time
import logging
import argparse
 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

with open(BASE_DIR+'\crtfc_key.txt') as key_data:
    CRTFC_KEY =key_data.readlines()

class Josim:
    def __init__(self, corp_name, corp_code):

        self.key_index = 1
        
        self.corp_name = corp_name
        self.corp_code = corp_code

        self.create_logger()

        #현재 날짜 
        now = datetime.datetime.now()
        self.base_year = now.year
        self.base_day = str(self.base_year-1)+'0101'

        # thstrm 당기 frmtrm 전기 
        self.amount_code = ['thstrm_amount', 'frmtrm_amount']
        # 사업보고서 분기별 코드 분류 1분기 11013, 2분기 11012, 3분기 11014, 사업보고서 11011
        self.report_codes = ["11011","11014","11012","11013"]
        self.report_name = [""]

        self.logger.info(corp_name+' / '+corp_code)
        self.corp_class = self.find_corp_class()
        self.logger.info('get corp class')
        # 상장된 주식이 아니면 실행 종료
        if (self.corp_class == 'E') or (self.corp_class == 'N'):
            self.logger.warning('not a listed stock')
        else:
            #DB 연결
            self.conn = pg2.connect(host = 'localhost', dbname = 'postgres', user= 'postgres', password= 'pangu', port='5432')
            self.cur = self.conn.cursor()
            self.logger.info('DB 연결')

            #공시정보확인
            self.last_report = self.check_disclosure()
            self.logger.info('get last report name')

            #DB에서 해당 기업 최신 데이터 확인 
            self.last_db_data = self.check_db()
            self.logger.info('get last db data')
            
            #만약 데이터가 최신 상태라면 추가 작업 없음
            if self.last_report == self.last_db_data:
                self.logger.info('latest status')
            else:
                #전체를 다시 받는게 아니라 부족한 부분만 받도록 변경
                self.report_data_array = self.get_report()
                self.logger.info("get corp's reports (json)")
                self.parse_report_data_array()
                self.logger.info('parsing json')

    #로깅 설정
    def create_logger(self):
        # Create Logger
        self.logger = logging.getLogger('test_logger')
 
        # Check handler exists
        if len(self.logger.handlers) > 0:
            return self.logger # Logger already exists

        self.logger.setLevel(logging.INFO)

        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        self.ch = logging.StreamHandler()
        self.ch.setFormatter(self.formatter)
        self.ch.setLevel(logging.INFO)

        self.fh = logging.FileHandler(filename= os.path.join(BASE_DIR,'info_log.log'))
        self.fh.setFormatter(self.formatter)
        self.fh.setLevel(logging.INFO)

        self.logger.addHandler(self.ch)
        self.logger.addHandler(self.fh)

    #corp code를 이용하여 corp class 반환 (유가, 코스닥 )
    def find_corp_class(self):
        cls_url = f'https://opendart.fss.or.kr/api/list.json?crtfc_key={CRTFC_KEY[self.key_index]}&corp_code={self.corp_code}&bgn_de={self.base_day}&last_reprt_at=N'
        response = requests.get(cls_url)
        data = json.loads(response.content)
        try:
            if data['status'] == '000':
                corp_cls = data['list'][0]['corp_cls'].replace(",","")
            elif data['status'] == '020':
                if self.key_index != 4:
                    self.key_index += 1
                    self.logger.info(f'api key index changed to {self.key_index-1} -> {self.key_index}')
                    return self.find_corp_class()
                else:
                    self.key_index = 0
                    self.logger.info('api key index changed to 4 -> 0')
                    return self.find_corp_class()
            else:
                corp_cls = 'E'

        except KeyError:
            corp_cls = 'E'
        return corp_cls

    #정기 공시 중 최상위 항목 반환 최근 공시와 현재 DB간 괴리 확인용
    def check_disclosure(self):
        url_disclosure = f'https://opendart.fss.or.kr/api/list.json?crtfc_key={CRTFC_KEY[self.key_index]}&corp_code={self.corp_code}&bgn_de=20200101&end_de=20210112&pblntf_ty=A&corp_cls={self.corp_class}&page_no=1&page_count=10'
        disclosure_response = requests.get(url_disclosure)
        disclosure_data = json.loads(disclosure_response.content)

        try:
            if disclosure_data['status'] == '000':
                last_report = disclosure_data['list'][0]['report_nm']
            elif disclosure_data['status'] == '020':
                if self.key_index != 4:
                    self.key_index += 1
                    self.logger.info(f'api key index changed to {self.key_index-1} -> {self.key_index}')
                    return self.check_disclosure()
                else:
                    self.key_index = 0
                    self.logger.info('api key index changed to 4 -> 0')
                    return self.check_disclosure()
            else:
                last_report = ''
        except KeyError:
            last_report = ''
            self.logger.warning('KeyError occured')
        return last_report

    #DB에서 상태가 정상인 데이터 중 연도와 보고서 종류( 반기, 분기, 사업 보고서)를 반환하여 공시 정보의 형태와 동일하게 맞춰줌
    def check_db(self):
        self.cur.execute(f"SELECT year,rept_code FROM dart_table_test WHERE corp_name = \'{self.corp_name}\' and data_status = '000'")
        db_result = self.cur.fetchall()
        if len(db_result) == 0:
            return 0
        else:
            result_dict = {'11013' : '분기보고서 ('+ db_result[0][0]+'.03)' ,'11012': '반기보고서 ('+ db_result[0][0]+'.06)', '11014' :'분기보고서 ('+ db_result[0][0]+'.09)', '11011' :'사업보고서 ('+ db_result[0][0]+'.12)'}
            result = result_dict[db_result[0][1]]
            return result
    
    # 연도별 보고서 받기
    def get_report(self):
        delta_year = [0,1,2,3,4,5]

        report_data_array = []
        for delta in delta_year:
            for report_code in self.report_codes:
                url = f"https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?crtfc_key={CRTFC_KEY[self.key_index]}&corp_code={self.corp_code}&bsns_year={self.base_year-delta}&reprt_code={report_code}"
                # GET 요청을 통한 json 받기
                response = requests.get(url)
                data = json.loads(response.content)
                report_data_array.append(data)
        return report_data_array

    def parse_report_data_array(self):
        self.data_status = []

        # 항목 초기화
        sales = 0
        net_income = 0
        capital = 0
        total_assets = 0
        ownership = 0
        business_profit = 0
        
        # account name에 맞게 당기, 전기 값 parsing
        for idx in np.arange(len(self.report_data_array)):
            if self.report_data_array[idx]['status'] == '000':
                self.data_status.append(1)
                for sub_data in self.report_data_array[idx]['list']:
                    try:
                        if sub_data['account_nm'] == '매출액':
                            sales = int(sub_data['thstrm_amount'].replace(",",""))
                        elif sub_data['account_nm'] == '법인세차감전 순이익':
                            net_income = int(sub_data['thstrm_amount'].replace(",",""))
                        elif sub_data['account_nm'] == '자본금':
                            capital = int(sub_data['thstrm_amount'].replace(",",""))
                        elif sub_data['account_nm'] == '자산총계':
                            total_assets = int(sub_data['thstrm_amount'].replace(",",""))
                        elif sub_data['account_nm'] == '자본총계':
                            ownership = int(sub_data['thstrm_amount'].replace(",",""))
                        elif sub_data['account_nm'] == '영업이익':
                            business_profit = int(sub_data['thstrm_amount'].replace(",",""))
                    except ValueError:
                        self.logger.warning('ValueError')
                        pass
                
               #DB로 데이터 저장
                self.cur.execute(f"INSERT\
                INTO dart_table_test\
                (data_status, corp_name, corp_cls, year, rept_code, sales_amount, net_income_amount, capital_amount, total_assets_amount, ownership_amount, business_profit_amount)\
                VALUES\
                (\'{self.report_data_array[idx]['status']}\',\'{self.corp_name}\', \'{self.corp_class}\', \'{self.base_year-int(idx/4)}\', \'{self.report_codes[idx%4]}\', {sales},{net_income},{capital},{total_assets},{ownership},{business_profit})")
                self.conn.commit()
            
            elif self.report_data_array[idx]['status'] == '020':
                if self.key_index != 4:
                    self.key_index += 1
                    self.logger.info(f'api key index changed to {self.key_index-1} -> {self.key_index}')
                    return self.parse_report_data_array()
                else:
                    self.key_index = 0
                    self.logger.info('api key index changed to 4 -> 0')
                    return self.parse_report_data_array()

            else:
                self.data_status.append(0)
                self.cur.execute(f"INSERT\
                INTO dart_table_test\
                (data_status, corp_name, corp_cls, year, rept_code, sales_amount, net_income_amount, capital_amount, total_assets_amount, ownership_amount, business_profit_amount)\
                VALUES\
                (\'{self.report_data_array[idx]['status']}\', \'{self.corp_name}\', \'{self.corp_class}\', \'{self.base_year-int(idx/4)}\', \'{self.report_codes[idx%4]}\', null,null,null,null,null,null)")

                self.conn.commit()

        self.cur.close()
        self.conn.close()

if __name__ == '__main__':

    # # XML에 있는 기업명 전체 반환하여 실행
    # tree = ET.parse(os.path.join(BASE_DIR,'corp_num','CORPCODE_RE.xml'))
    # root = tree.getroot()
    # length = len(root.findall('list'))

    # for idx,country in enumerate(root.iter("list")):
    #     josim = Josim(country.findtext("corp_name"),country.findtext("corp_code"))
    #     print(str(idx)+' / '+str(length))

    josim = Josim('신라젠','00919966')

    # #DB 연결 
    # conn = pg2.connect(host = 'localhost', dbname = 'postgres', user= 'postgres', password= 'pangu', port='5432')
    # cur = conn.cursor()

    # #현재 DB에 저장된 값 중 Status 가 정상인 것 전부 반환 
    # cur.execute("SELECT * FROM dart_table_test WHERE corp_name = '신라젠' and data_status='000'")
    # result = cur.fetchall()
    # print(result)
    
    # cur.close()
    # conn.close()
