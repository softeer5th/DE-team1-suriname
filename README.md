# DE-Team1-suriname

## 🚘 현대기아차 이슈 모니터링 서비스

```
현대기아 자동차 관련 큰 이슈가 발생하였을 때 해당 이슈에 대한 대중들의 반응을 빠르고
정확하게 파악하기 위한 모니터링 및 알림 서비스입니다.
```
## 📹 시연 영상
- [Youtube Link](https://www.youtube.com/watch?v=EK1W47P1QxY)

## ✨ 기능
- `이슈 주의도`
  - 이슈 관련 데이터(뉴스·커뮤니티 언급량 및 반응)를 종합한 점수를 통해 이슈의 심각도를 나타내는 지표입니다.
  - 이슈 주의도는 점수에 따라 관심 🟡, 주의 🟠, 긴급 🔴 3단계로 구분됩니다.

- `이슈 감지 알림 기능`
  - 이슈가 처음으로 감지된 순간 슬랙으로 이슈 담당팀에게 알림이 전송됩니다.
  - 이슈 내용, 주의도, 최초 발생 시간, 대시보드 링크가 알림으로 제공됩니다.

- `이슈 대시보드` 
  - 이슈에 대한 통계지표를 보여줍니다. 

- `개발자 알림 기능`
  - 파이프라인에 에러가 발생할 경우 슬랙으로 개발팀에게 알림이 전송됩니다.


<table>
  <tr>
    <td style="width: 50%; text-align: center;">
      <img src="https://github.com/user-attachments/assets/54650c0a-f1c6-4ae1-8b6b-d88d8f4b9b8d" style="width: 100%;">
    </td>
    <td style="width: 50%; text-align: center;">
      <img src="https://github.com/user-attachments/assets/b4220447-790f-4b1b-9d83-2ea9095465e0" style="width: 100%;">
    </td>
  </tr>
  <tr>
    <td style="width: 50%; vertical-align: top; text-align: center;">
      슬랙 알림
    </td>
        <td style="width: 50%; vertical-align: top; text-align: center;">
      대시보드
    </td>
  </tr>
</table>

## 🖥 파이프라인
  <img src="https://github.com/user-attachments/assets/04966e8e-c1fe-47a8-8ac3-5b83ba9926e9" style="width: 100%;">

## 프로젝트 구조
- [extract/](https://github.com/softeer5th/DE-team1-suriname/tree/main/extract): 뉴스 및 커뮤니티 데이터 추출
- [transform/](https://github.com/softeer5th/DE-team1-suriname/tree/main/transform): 사건과 관련되어 있는 데이터 파악 및 점수 산정과 같은 transform 과정
- [load/](https://github.com/softeer5th/DE-team1-suriname/tree/main/load): RDS 및 Redshift에 데이터 적재
- [sns/](https://github.com/softeer5th/DE-team1-suriname/tree/main/sns): 슬랙알림에 관련된 파일들
- [airflow/](https://github.com/softeer5th/DE-team1-suriname/tree/main/airflow) : 에어플로우 세팅 및 DAG


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
