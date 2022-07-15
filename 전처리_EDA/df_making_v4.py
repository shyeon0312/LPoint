#!/usr/bin/env python
# coding: utf-8

# ## [ 함수 설명 ]
# ### 변수 설명
# - 입력 변수 설명
#     - data_dir : raw data(6개)가 들어있는 폴더 위치. 해당 폴더에는 raw data만 들어있어야 함.
#     - save_dir : 생성된 dataframe을 저장시킬 폴더 위치.
# - 출력 변수 설명 (변수로 바로 선언하는 경우를 위해)
#     - df_제휴사: df3 기준으로 df1, df5, df6 merge된 dataframe
#     - df_유통사: df2 기준으로 df1, df2, df5, df6 merge된 dataframe
#     - LP_wo2   : LPoint without df2, df6 중 df_유통사(df2 기준)에 포함되지 않은 데이터
#     - LP_wo3   : LPoint without df3, df6 중 df_제휴사(df3 기준)에 포함되지 않은 데이터
# 
# ### 사용 방법
# - input값인 data_dir, save_dir 지정 및 함수 변수로 입력
# - csv파일로 저장이 필요하지 않은 경우, 함수 선언부 하단에 있는 csv파일 저장 코드를 지우는 것을 추천드림 (약 5~10초 소요)
# - 변수로 바로 선언하지 않는다면, 함수 선언부 맨 마지막 return행 지우세요

# In[1]:


import os
import pandas as pd
import numpy as np
import math
import datetime


# In[2]:


# 전체 데이터 셋
def df_making(data_dir, save_dir):
    
    #휴일 구하는 함수 선언
    def get_holidays(y, m, d):
        holidays=list([[1, 1],[2, 11],[2, 12],[2, 13],[3, 1],[5, 5],[5, 19],[6, 6],[8, 15],[9, 20],[9, 21],[9, 22],[10, 3],[10, 9],[12, 25]])

        if [m, d] in holidays:  # 공휴일 날짜 안에 속하면 True
            return 1
        date = datetime.date(y, m, d)
        return int(date.isoweekday() > 5) #토, 일요일이면 True
    
    # 폴더 위치 확인
    filename = list()
    for f in os.listdir(data_dir):
        filename.append(f)

    # 파일 읽어오기
    df1 = pd.read_csv(os.path.join(data_dir, filename[0])) #고객데모정보
    df2 = pd.read_csv(os.path.join(data_dir, filename[1])) #유통사 상품 구매 내역
    df3 = pd.read_csv(os.path.join(data_dir, filename[2])) #제휴사 서비스 이용 내역
    df4 = pd.read_csv(os.path.join(data_dir, filename[3])) #유통사 상품 카테고리 마스터
    df5 = pd.read_csv(os.path.join(data_dir, filename[4])) #유통사/제휴사 점포 마스터
    df6 = pd.read_csv(os.path.join(data_dir, filename[5])) #엘페이 결제 내역

    # 컬럼명 변경
    df1.rename(columns={'cust':'고객번호', 'ma_fem_dv':'성별', 'ages':'연령대', 'zon_hlv':'거주지'}, inplace=True)
    df2.rename(columns={'cust':'고객번호', 'rct_no':'영수증', 'chnl_dv':'채널', 'cop_c':'유통사', 'pd_c':'상품코드', 'br_c':'점포코드', 'de_dt':'이용일자', 'de_hr':'이용시간', 'buy_am':'이용금액', 'buy_ct':'구매수량'}, inplace=True)
    df3.rename(columns={'cust':'고객번호', 'rct_no':'영수증', 'cop_c':'제휴사', 'br_c':'점포코드', 'de_dt':'이용일자', 'vst_dt':'방문일자', 'de_hr':'이용시간', 'buy_am':'이용금액', 'chnl_dv':'채널'}, inplace=True)
    df4.rename(columns={'pd_c':'상품코드', 'pd_nm':'소분류', 'clac_hlv_nm':'대분류', 'clac_mcls_nm':'중분류'}, inplace=True)
    df5.rename(columns={'br_c':'점포코드', 'cop_c':'제휴사', 'zon_hlv':'점포_대분류', 'zon_mcls':'점포_중분류', 'chnl_dv':'채널'}, inplace=True)
    df6.rename(columns={'cust':'고객번호', 'cop_c':'제휴사', 'rct_no':'영수증', 'de_dt':'이용일자', 'de_hr':'이용시간', 'buy_am':'이용금액', 'chnl_dv':'채널'}, inplace=True)

    ## ---------------df_제휴사 (df3 기준 merge)---------------
    print('---------------df_제휴사 (df3 기준 merge)---------------')
    # 1. df1, df3를 "고객번호" 기준으로 합
    df_제휴사 = pd.merge(df3, df1, how='inner', on='고객번호')

    if(len(df_제휴사) == len(df3)):
        print("df1과 df3이 성공적으로 merge 되었습니다.")
    else:
        print("!! df1과 df3 merge과정 확인 필요 !!")

    # 2. df_제휴사 와 df5 를 "점포코드" 기준으로 합 (이 때, df5의 제휴사는 고려하지 않았음)
    test = pd.merge(df_제휴사.drop('제휴사', axis=1), df5, how='left', on='점포코드')
    df_제휴사_test = pd.merge(df_제휴사, df5, how='left', on='점포코드')
    if(len(test) == len(df_제휴사)):
        print("df_제휴사와 df5가 성공적으로 merge 되었습니다.")
        df_제휴사 = test.copy()
        del test
    else:
        print("!! df_제휴사 + df5 merge과정 확인 필요 !!")

    # 3. df_제휴사 + LPAY 이용 여부
    # 구분코드 = 고객번호 + 이용일자 + 이용시간 + 이용금액 + 채널 + 제휴사
    df_제휴사['구분코드'] = df_제휴사['고객번호'] + df_제휴사['이용일자'].astype('str') + df_제휴사['이용시간'].astype('str') + df_제휴사['이용금액'].astype('str') + df_제휴사['채널'].astype('str') + df_제휴사['제휴사'].astype('str') 
    df6['구분코드'] = df6['고객번호'] + df6['이용일자'].astype('str') + df6['이용시간'].astype('str') + df6['이용금액'].astype('str') + df6['채널'].astype('str') + df6['제휴사'].astype('str')

    # 구분코드가 완전히 동일한 경우한 merge한다
    # merge된 경우만 확인하기 위해 inner join
    df3_6 = pd.merge(df_제휴사, df6.drop(['고객번호', '이용일자', '이용시간', '이용금액', '채널'], axis=1), how='inner', on='구분코드')
    # LPOINT 이용한 경우의 영수증 번호만 저장
    lpoint_yes = list(df3_6['영수증_x'].drop_duplicates())
    
    # 3-1. LPOINT 사용 여부 칼럼 생성
    df_제휴사['LPoint'] = [0] * len(df_제휴사)
    idx = df_제휴사.loc[df_제휴사['영수증'].isin(lpoint_yes)].index
    df_제휴사.loc[idx, 'LPoint'] = 1
    print("df_제휴사와 df6이 성공적으로 merge 되었습니다.")

    # 3-2. LPoint 사용 안한 경우 남겨두기
    lpointonly = list(set(df6['영수증']) - set(list(df3_6['영수증_y'].drop_duplicates())))
    LP_wo3 = df6.loc[df6['영수증'].isin(lpointonly)]
    print('LPoint without df3 생성 완료')

    # 4. 휴일 column 추가 ('방문일자'가 토, 일, 공휴일이면 1, 아니면 0)
    df_제휴사['휴일'] = df_제휴사['방문일자'].apply(lambda x: get_holidays(int(str(x)[:4]), int(str(x)[4:6]), int(str(x)[6:])))
    
    ## ---------------df_유통사 (df2 기준 merge)---------------
    print('---------------df_유통사 (df2 기준 merge)---------------')
    # 1. df1, df2를 "고객번호" 기준으로 merge
    df_유통사 = pd.merge(df2, df1, how='inner', on='고객번호')

    if(len(df_유통사) == len(df2)):
        print("df1과 df2이 성공적으로 merge 되었습니다.")
    else:
        print("!! df1과 df2 merge과정 확인 필요 !!")

    # 2. df_유통사, df4를 "상품코드" 기준으로 merge
    test = pd.merge(df_유통사, df4, how='inner', on='상품코드')
    if(len(test) == len(df_유통사)):
        print("df_유통사 와 df4가 성공적으로 merge 되었습니다.")
        df_유통사 = test.copy()
        del test
    else:
        print("!! df_유통사과 df4 merge과정 확인 필요 !!")

    # 3. df_유통사, df5를 "점포코드" 기준으로 merge
    test = pd.merge(df_유통사, df5, how='left', on='점포코드') #점포코드에 null값있으므로 left join
    if(len(test) == len(df_유통사)):
        print("df_유통사 와 df5가 성공적으로 merge 되었습니다.")
        df_유통사 = test.copy()
        del test
    else:
        print("!! df_유통사과 df5 merge과정 확인 필요 !!")

    # 4. df_유통사 + LPAY 이용 여부
    df_유통사['구분코드'] = df_유통사['고객번호'] + df_유통사['이용일자'].astype('str') + df_유통사['이용시간'].astype('str') + df_유통사['이용금액'].astype('int').astype('str') + df_유통사['채널'].astype('str') + df_유통사['제휴사'].astype('str')
    df6['구분코드'] = df6['고객번호'] + df6['이용일자'].astype('str') + df6['이용시간'].astype('str') + df6['이용금액'].astype('str') + df6['채널'].astype('str') + df6['제휴사'].astype('str')

    # 구분코드가 완전히 동일한 경우한 merge한다
    # merge된 경우만 확인하기 위해 inner join
    df2_6 = pd.merge(df_유통사, df6.drop(['고객번호', '이용일자', '이용시간', '이용금액', '채널'], axis=1), how='inner', on='구분코드')

    # LPOINT 이용한 경우의 영수증 번호만 저장
    lpoint_yes = list(df2_6['영수증_x'].drop_duplicates())

    # 4-1. LPOINT 사용 여부 칼럼 생성
    df_유통사['LPoint'] = [0] * len(df_유통사)
    idx = df_유통사.loc[df_유통사['영수증'].isin(lpoint_yes)].index
    df_유통사.loc[idx, 'LPoint'] = 1
    print("df_유통사와 df6이 성공적으로 merge 되었습니다.")

    # 4-2. LPoint 사용 안한 경우 남겨두기
    lpointonly = list(set(df6['영수증']) - set(list(df3_6['영수증_y'].drop_duplicates())))
    LP_wo2 = df6.loc[df6['영수증'].isin(lpointonly)]
    print('LPoint without df2 생성 완료')
    # 4. 휴일 column 추가 ('이용일자'가 토, 일, 공휴일이면 1, 아니면 0)
    df_유통사['휴일'] = df_유통사['이용일자'].apply(lambda x: get_holidays(int(str(x)[:4]), int(str(x)[4:6]), int(str(x)[6:])))
    
    #'구분코드' 열 삭제
    df_제휴사.drop('구분코드', axis=1, inplace = True)
    df_유통사.drop('구분코드', axis=1, inplace = True)
    LP_wo2.drop('구분코드', axis=1, inplace = True)
    LP_wo3.drop('구분코드', axis=1, inplace = True)
    
    ## ---------------csv 파일로 저장---------------
    df_제휴사.to_csv(os.path.join(save_dir, "df_제휴사.csv"), index=False)
    df_제휴사.to_csv(os.path.join(save_dir, "df_제휴사.csv"), index=False)
    df_유통사.to_csv(os.path.join(save_dir, "df_유통사.csv"), index=False)
    LP_wo2.to_csv(os.path.join(save_dir, "LP_wo2.csv"), index=False)
    LP_wo3.to_csv(os.path.join(save_dir, "LP_wo3.csv"), index=False)
    print('데이터 파일이 저장되었습니다.')
    
    return df_제휴사, df_유통사, LP_wo2, LP_wo3 #생성된 dataframe을 바로 이용하는 경우를 위해,


# ## [ f유통사_고객별 ]
# ### 변수 설명
# - 입력 변수 설명
#     - df : df_유통사.csv 파일을 읽은 후 삽입
# - 출력 변수 설명
#     - merged : df_유통사_고객별

# In[3]:


def f유통사_고객별(df):
    df = df.sort_values('고객번호')
    merged = df[['고객번호','성별','연령대','거주지']].drop_duplicates() # 고객정보

    # LPoint ratio
    LPoint = df[['고객번호', 'LPoint']].groupby('고객번호', as_index=False).mean()
    merged = pd.merge(merged, LPoint, how='left', on='고객번호')
    print('LPoint raio 완료')
    
    # 유통사 휴일
    휴일 = df[['고객번호','휴일']].groupby('고객번호', as_index=False).mean()
    merged = pd.merge(merged, 휴일, how='left', on='고객번호')
    print('휴일 완료')

    # 유통사 비율
    test = pd.get_dummies(df[['고객번호', '유통사']], columns=['유통사'])
    유통사 = test.groupby(['고객번호'], as_index=False).sum()
    유통사['size'] = df.groupby(['고객번호'], as_index=False).size()['size']

    if(sum(유통사['size']!=(유통사['유통사_A01']+유통사['유통사_A02']+유통사['유통사_A03']+유통사['유통사_A04']+유통사['유통사_A05']+유통사['유통사_A06']))==0):
        # 제휴사가 nan 인 경우도 있음
        유통사['유통사_A01'] = 유통사['유통사_A01']/유통사['size']
        유통사['유통사_A02'] = 유통사['유통사_A02']/유통사['size']
        유통사['유통사_A03'] = 유통사['유통사_A03']/유통사['size']
        유통사['유통사_A04'] = 유통사['유통사_A04']/유통사['size']
        유통사['유통사_A05'] = 유통사['유통사_A05']/유통사['size']
        유통사['유통사_A06'] = 유통사['유통사_A06']/유통사['size']
        유통사.drop('size', axis=1, inplace=True)
        merged = pd.merge(merged, 유통사, how='left', on='고객번호')
        print('유통사 완료')
    else:
        print('!!! 유통사 확인 필요')
        display(유통사[유통사['size']!=(유통사['유통사_A01']+유통사['유통사_A02']+유통사['유통사_A03']+유통사['유통사_A04']+유통사['유통사_A05']+유통사['유통사_A06'])])
        
    ##----------------코드합침----------------
    # 채널 비율 계산
    test2 = pd.get_dummies(df[['고객번호', '채널']], columns=['채널'])

    channel = test2.groupby(['고객번호'], as_index=False).sum()
    channel['size'] = test2.groupby(['고객번호'], as_index=False).size()['size']
    
    # 계산이 제대로 되었는지 확인
    if sum(channel['size'] != (channel['채널_1'] + channel['채널_2']))==0:
        channel['채널_1'] = channel['채널_1'] / channel['size']
        channel['채널_2'] = channel['채널_2'] / channel['size']
        channel.drop(['size'], axis=1, inplace=True)
        #display(channel)
        merged = pd.merge(merged, channel, how='left', on='고객번호')
        print('채널 완료')
    else:
        print('!! 채널 확인 필요')

    # 이용일자
    test2 = df[['고객번호', '이용일자']]
    test2['계절'] = test2['이용일자'].apply(lambda x: (int(str(x)[4:6])//4)+1)

    test2 = pd.get_dummies(test2, columns=['계절']).rename(columns={'계절_1':'봄', '계절_2':'여름', '계절_3':'가을', '계절_4':'겨울'})
    test2.drop('이용일자', axis=1, inplace=True)
    test2['봄'] = test2['봄'].astype('int')
    test2['여름'] = test2['여름'].astype('int')
    test2['가을'] = test2['가을'].astype('int')
    test2['겨울'] = test2['겨울'].astype('int')
    season = test2.groupby(['고객번호'], as_index=False).sum()
    season['size'] = test2.groupby(['고객번호'], as_index=False).size()['size']

    if sum(season['size']!=season['봄']+season['여름']+season['가을']+season['겨울'])==0:
        season['봄'] = season['봄']/season['size']
        season['여름'] = season['여름']/season['size']
        season['가을'] = season['가을']/season['size']
        season['겨울'] = season['겨울']/season['size']
        season.drop('size', axis=1, inplace=True)
        #display(season)

        merged = pd.merge(merged, season, how='left', on='고객번호')
        print('이용일자 -> 계절 완료')
    else:
        print('!!! 이용일자 확인 필요')

    # 이용시간
    test2 = df[['고객번호','이용시간']]
    test2['이용시간'] = test2['이용시간'].apply(lambda x: (x//6)+1)

    test2 = pd.get_dummies(test2, columns=['이용시간']).rename(columns={'이용시간_1':'새벽', '이용시간_2':'오전', '이용시간_3':'오후', '이용시간_4':'저녁'})
    test2['새벽'] = test2['새벽'].astype('int')
    test2['오전'] = test2['오전'].astype('int')
    test2['오후'] = test2['오후'].astype('int')
    test2['저녁'] = test2['저녁'].astype('int')
    time = test2.groupby(['고객번호'], as_index=False).sum()
    time['size'] = test2.groupby(['고객번호'], as_index=False).size()['size']
    
    if sum(time['size']!=time['새벽']+time['오전']+time['오후']+time['저녁'])==0:
        time['새벽'] = time['새벽']/time['size']
        time['오전'] = time['오전']/time['size']
        time['오후'] = time['오후']/time['size']
        time['저녁'] = time['저녁']/time['size']
        #display(time)

        merged = pd.merge(merged, time, how='left', on='고객번호')
        print('시간대 변경 완료')
    else:
        print('!!! 시간대(계절) 확인 필요')

    # 이용금액
    test2 = df[['고객번호','이용금액']]
    money = test2.groupby(['고객번호'], as_index=False).sum().rename(columns={'이용금액':'이용금액_총합'})
    money['이용금액_평균'] = test2.groupby(['고객번호'], as_index=False).mean()['이용금액'].apply(lambda x: round(x, 2))
    #display(money)
    merged = pd.merge(merged, money, how='left', on='고객번호')
    print('이용금액 총합/평균 계산 완료')
    
    merged['이용건수'] = time['size'].copy()

    return merged        


# ## [ 제휴사_고객별 ]
# ### 변수 설명
# - 입력 변수 설명
#     - df : df_제휴사.csv 파일을 읽은 후 삽입
# - 출력 변수 설명
#     - merged : df_제휴사_고객별

# In[4]:


def f제휴사_고객별(df):
    df = df.sort_values('고객번호')
    merged = df[['고객번호','성별','연령대','거주지']].drop_duplicates() # 고객정보

    # LPoint ratio
    LPoint = df[['고객번호', 'LPoint']].groupby('고객번호', as_index=False).mean()
    merged = pd.merge(merged, LPoint, how='left', on='고객번호')
    print('LPoint raio 완료')
    
    # 제휴사 휴일
    휴일 = df[['고객번호','휴일']].groupby('고객번호', as_index=False).mean()
    merged = pd.merge(merged, 휴일, how='left', on='고객번호')
    print('휴일 완료')

    # 제휴사 비율 -> nan있음
    test = df[['고객번호', '제휴사']]
    test = pd.get_dummies(test, columns=['제휴사'])
    test = test.groupby(['고객번호'], as_index=False).sum()
    test['size'] = df.groupby(['고객번호'], as_index=False).size()['size']
    test['제휴사_B'] = test['제휴사_B01']/test['size']
    test['제휴사_C'] = (test['제휴사_C01']+test['제휴사_C02'])/test['size']
    test['제휴사_D'] = (test['제휴사_D01']+test['제휴사_D02'])/test['size']
    test['제휴사_E'] = test['제휴사_E01']/test['size']
    test.drop('size', axis=1, inplace=True)
    test.drop(['제휴사_B01', '제휴사_C01', '제휴사_C02', '제휴사_D01', '제휴사_D02', '제휴사_E01'], axis=1, inplace=True)

    merged = pd.merge(merged, test, how='left', on='고객번호')
    print('제휴사 완료')

    ##-------코드합침--------
    # 채널 비율 계산
    test2 = pd.get_dummies(df[['고객번호', '채널']], columns=['채널'])

    channel = test2.groupby(['고객번호'], as_index=False).sum()
    channel['size'] = test2.groupby(['고객번호'], as_index=False).size()['size']
    
    # 계산이 제대로 되었는지 확인
    if sum(channel['size'] != (channel['채널_1'] + channel['채널_2']))==0:
        channel['채널_1'] = channel['채널_1'] / channel['size']
        channel['채널_2'] = channel['채널_2'] / channel['size']
        channel.drop(['size'], axis=1, inplace=True)
        #display(channel)
        
        merged = pd.merge(merged, channel, how='left', on='고객번호')
        print('채널 완료')
    else:
        print('!!! 채널 확인 필요')

    # 이용일자
    test2 = df[['고객번호', '이용일자']]
    test2['계절'] = test2['이용일자'].apply(lambda x: (int(str(x)[4:6])//4)+1)

    test2 = pd.get_dummies(test2, columns=['계절']).rename(columns={'계절_1':'봄', '계절_2':'여름', '계절_3':'가을', '계절_4':'겨울'})
    test2['봄'] = test2['봄'].astype('int')
    test2['여름'] = test2['여름'].astype('int')
    test2['가을'] = test2['가을'].astype('int')
    test2['겨울'] = test2['겨울'].astype('int')
    test2.drop('이용일자', axis=1, inplace=True)

    season = test2.groupby(['고객번호'], as_index=False).sum()
    season['size'] = df.groupby(['고객번호'], as_index=False).size()['size']

    if sum(season['size']!=(season['봄']+season['여름']+season['가을']+season['겨울']))==0:
        season['봄'] = season['봄']/season['size']
        season['여름'] = season['여름']/season['size']
        season['가을'] = season['가을']/season['size']
        season['겨울'] = season['겨울']/season['size']
        season.drop('size', axis=1, inplace=True)
        #display(season)

        merged = pd.merge(merged, season, how='left', on='고객번호')
        print('이용일자 -> 계절 완료')
    else:
        print('!!! 이용일자 확인 필요')
        display(season[season['size']!=season['봄']+season['여름']+season['가을']+season['겨울']])

    # 이용시간
    test2 = df[['고객번호','이용시간']]
    test2['이용시간'] = test2['이용시간'].apply(lambda x: (x//6)+1)

    test2 = pd.get_dummies(test2, columns=['이용시간']).rename(columns={'이용시간_1':'새벽', '이용시간_2':'오전', '이용시간_3':'오후', '이용시간_4':'저녁'})
    test2['새벽'] = test2['새벽'].astype('int')
    test2['오전'] = test2['오전'].astype('int')
    test2['오후'] = test2['오후'].astype('int')
    test2['저녁'] = test2['저녁'].astype('int')
    time = test2.groupby(['고객번호'], as_index=False).sum()
    time['size'] = test2.groupby(['고객번호'], as_index=False).size()['size']

    if sum(time['size']!=(time['새벽']+time['오전']+time['오후']+time['저녁']))==0:
        time['새벽'] = time['새벽']/time['size']
        time['오전'] = time['오전']/time['size']
        time['오후'] = time['오후']/time['size']
        time['저녁'] = time['저녁']/time['size']
        #display(time)

        merged = pd.merge(merged, time, how='left', on='고객번호')
        print('시간대 변경 완료')
    else:
        print('!!! 이용시간 확인 필요')
        display(time[time['size']!=(time['새벽']+time['오전']+time['오후']+time['저녁'])])

    # 이용금액
    test2 = df[['고객번호','이용금액']]
    money = test2.groupby(['고객번호'], as_index=False).sum().rename(columns={'이용금액':'이용금액_총합'})
    money['이용금액_평균'] = test2.groupby(['고객번호'], as_index=False).mean()['이용금액'].apply(lambda x: round(x, 2))
    #display(money)
    merged = pd.merge(merged, money, how='left', on='고객번호')
    print('이용금액 총합/평균 계산 완료')
    merged['이용건수'] = time['size'].copy()
    
    return merged


# ## [ 사용 예시 ]

# In[5]:


import time
start = time.time()
data_dir = "../../Data/raw_data"  # 데이터 들어있는 폴더 위치
save_dir = "../../Data"           # 가공된 데이터 파일을 저장시킬 위치
df_제휴사, df_유통사, LP_wo2, LP_wo3 = df_making(data_dir, save_dir)
print('\n 소요시간 : ', round(time.time()-start), '초')


# In[6]:


start = time.time()
#df_유통사 = pd.read_csv('../../Data/df_유통사.csv')
df_유통사_고객별 = f유통사_고객별(df_유통사)
df_유통사_고객별.to_csv('../../Data/df_유통사_고객별.csv')
print('\n 소요시간 : ', round(time.time()-start), '초')


# In[7]:


start = time.time()
#df_제휴사 = pd.read_csv('../../Data/df_제휴사.csv')
df_제휴사_고객별 = f제휴사_고객별(df_제휴사)
df_제휴사_고객별.to_csv('../../Data/df_제휴사_고객별.csv')
print('\n 소요시간 : ', round(time.time()-start), '초')


# In[8]:


len(df_유통사_고객별)


# In[9]:


len(df_제휴사_고객별)


# In[10]:


len(df_유통사['고객번호'].drop_duplicates())


# In[11]:


len(df_제휴사['고객번호'].drop_duplicates())


# In[ ]:




