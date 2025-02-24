import pathlib
import textwrap

import google.generativeai as genai
import conf

GOOGLE_API_KEY= conf.GOOGLE_API_KEY

def get_score_from_gemini(car_model,accident,title,content) : 
    genai.configure(api_key=GOOGLE_API_KEY)
    
    model = genai.GenerativeModel('gemini-1.5-pro')

    data = f"""
                [커뮤니티 분석 요청]

                [할 것]
                너는 자동차 관련 글에서 특정 차량과 사고 유형에 대한 감성 분석을 수행하는 AI야.
                주어진 데이터를 기반으로 **차량 모델과 사고 유형**(예: '{car_model}' + '{accident}')에 대한 전체적인 감성을 분석해줘.

                각 행에는 제목(title), 본문(content)이 포함되어 있어.
                이 모든 요소를 종합하여 **해당 차량과 사고 유형에 대한 감성 점수**를 100(매우 부정적)에서 0(매우 긍정적)까지의 범위로 숫자로 평가해줘.
                긍부정은 현대기아 자동차 입장에서 판단을 해줘. 예를 들어 현대기아를 옹호하거나, 사건의 원인을 현대기아 자동차가 아닌 다른데 있으면 긍정적인거고, 현대기아차를 비판하거나 사건의 원인이라고 규정하면 있으면 부정적인거야.
                50에 가까울수록 중립적이며, 100에 가까울수록 부정적, 0에 가까울수록 긍정적인거야.
                반환은 반드시 0~100사이의 정수 숫자만 반환하고 아무런 말도 하지마.
                아래는 분석할 데이터야:

                ---
                **차량 모델:** {car_model}  
                **사고 유형:** {accident}  
                **제목:** {title}  
                **본문:** {content}
                
                ---
                """

    generation_config = {
        "temperature": 0.2,  # 낮을수록 일관된 결과
    }

    response = model.generate_content(data, generation_config=generation_config)
    return response.text