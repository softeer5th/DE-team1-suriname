# DE-Team1-suriname

## 🚘 현대기아차 이슈 모니터링 서비스

```
현대기아 자동차 관련 큰 이슈가 발생하였을 때 해당 이슈에 대한 대중들의 반응을 빠르고 정확하게 파악하기 위한 모니터링 및 알림 서비스입니다.
```

## ✨ 기능
- `이슈 주의도`
  - 이슈 관련 데이터(뉴스·커뮤니티 언급량 및 반응)를 종합한 점수를 통해 이슈의 심각도를 나타내는 지표입니다.
  - 이슈 주의도는 점수에 따라 관심 🟡, 주의 🟠, 긴급 🔴 3단계로 구분됩니다.

- `이슈 감지 알림 기능`
  - 이슈가 처음으로 감지된 순간 슬랙으로 이슈 담당팀 알림이 전송됩니다.
  - 이슈 내용, 주의도, 최초 발생 시간, 대시보드 링크가 알림으로 제공됩니다.

- `이슈 대시보드` 
  - 이슈에 대한 통계지표를 보여줍니다. 

- `개발자 알림 기능`
  - 파이프라인에 에러가 발생할 경우 슬랙으로 개발팀에게 알림이 전송됩니다.

++++ 이미지

## 📹 시연 영상
- [Youtube Link](https://www.youtube.com/@KiaKorea)

## 🖥 파이프라인

## 📝 협업 노션
- [Notion Link](https://tangy-gargoyle-943.notion.site/19033f08a6728006b538f59aee8d2ccd)

## 팀원
<table width="100%">
<tbody><tr>
    <td width="33.33%" align="center"><b>주수민</b></td>
    <td width="33.33%" align="center"><b>이준호</b></td>
    <td width="33.33%" align="center"><b>남선우</b></td>
</tr>
<tr>
    <td align="center"><a href="https://github.com/cleonno3o"><img src="https://github.com/cleonno3o.png" width="100" height="100" style="max-width: 100%;"></a></td>
    <td align="center"><a href="https://github.com/junoritto"><img src="https://github.com/junoritto.png" width="100" height="100" style="max-width: 100%;"></a></td>
    <td align="center"><a href="https://github.com/seonwoonam"><img src="https://github.com/seonwoonam.png" width="100" height="100" style="max-width: 100%;"></a></td>
</tr>
<tr>
    <td align="center"><a href="https://github.com/cleonno3o">@cleonno3o</a></td>
    <td align="center"><a href="https://github.com/junoritto">@junoritto</a></td>
    <td align="center"><a href="https://github.com/seonwoonam">@seonwoonam</a></td>
</tr>
<tr>
    <td align="center">DE</td>
    <td align="center">DE</td>
    <td align="center">DE</td>
</tr>
</tbody></table>


## 프로젝트 구조
📦DE-team1-suriname  
 ┣ 📂.github  
 ┣ 📂airflow  
 ┃ ┗ 📂dags  
 ┃ ┃ ┣ 📜community_dag.py  
 ┃ ┃ ┗ 📜news_dag.py  
 ┣ 📂extract  
 ┃ ┣ 📂community    
 ┃ ┃ ┣ 📂type  
 ┃ ┃ ┃ ┣ 📜__init__.py  
 ┃ ┃ ┃ ┗ 📜community_crawler.py  
 ┃ ┃ ┣ 📜Dockerfile    
 ┃ ┃ ┣ 📜bobae_crawler.py  
 ┃ ┃ ┣ 📜chrome-installer.sh  
 ┃ ┃ ┣ 📜dcinside_crawler.py  
 ┃ ┃ ┣ 📜femco_crawler.py  
 ┃ ┃ ┣ 📜lambda_function.py  
 ┃ ┃ ┣ 📜requirements.txt  
 ┃ ┣ 📂news  
 ┃ ┃ ┣ 📂crawler  
 ┃ ┃ ┃ ┣ 📜base_crawler.py  
 ┃ ┃ ┃ ┣ 📜kbs_crawler.py  
 ┃ ┃ ┃ ┣ 📜lambda_function.py  
 ┃ ┃ ┃ ┣ 📜sbs_crawler.py  
 ┃ ┃ ┃ ┣ 📜yna_crawler.py  
 ┃ ┃ ┃ ┗ 📜ytn_crawler.py  
 ┃ ┃ ┣ 📂type  
 ┃ ┃ ┃ ┣ 📜__init__.py  
 ┃ ┃ ┃ ┗ 📜news_crawler.py  
 ┃ ┃ ┣ 📜conf.py  
 ┃ ┃ ┣ 📜lambda_function.py  
 ┃ ┃ ┗ 📜test.ipynb  
 ┣ 📂load  
 ┃ ┣ 📂load_final_table  
 ┃ ┃ ┗ 📜lambda_function.py  
 ┃ ┣ 📂load_news  
 ┃ ┃ ┗ 📜lambda_function.py  
 ┃ ┗ 📂upload_to_redshift  
 ┃ ┃ ┣ 📜Dockerfile  
 ┃ ┃ ┣ 📜lambda_function.py  
 ┃ ┃ ┗ 📜requirements.txt  
 ┣ 📂sns  
 ┃ ┣ 📜slack_alert.py  
 ┃ ┗ 📜slack_alert_develop.py  
 ┣ 📂sql  
 ┃ ┗ 📜initiate_accumulate_table.sql  
 ┣ 📂transform  
 ┃ ┣ 📜emr_transform_community.py  
 ┃ ┣ 📜emr_transform_news.py  
 ┃ ┣ 📜lambda_emr_news_trigger.py  
 ┃ ┣ 📜lambda_task_monitor.py  
 ┃ ┗ 📜runner_emr_community.py  
 ┣ 📜.gitignore  
 ┗ 📜README.md  



