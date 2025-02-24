# DE-Team1-suriname
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

## 팀원 소개!
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

