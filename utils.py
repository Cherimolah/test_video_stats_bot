import re

from loader import client
from config import MODEL_NAME

with open('prompt.txt', 'r', encoding='utf-8') as file:
    prompt = file.read()


sql_code_pattern = re.compile(r'```sql\s*\n(?P<sql_code>.*?)\n```', re.DOTALL | re.IGNORECASE)


async def request_llm(text: str) -> str:
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {
                "role": "user",
                "content": text
            }
        ],
        extra_body={"reasoning": {"enabled": True}}
    )

    # Extract the assistant message with reasoning_details
    response = response.choices[0].message.content
    return response


def extract_sql(markdown_text: str) -> str | None:
    if x := re.match(sql_code_pattern, markdown_text):
        return x.group('sql_code')



async def check_sql(sql_code: str) -> bool:
    prompt_ = (f'Проверь этот SQL запрос на безопасность. Он должен содржать **только** SELECT запросы. Данные ни в коем'
               f'случае не должны быть изменены. Ответь одним словом OK, если этот запрос безопасно вызвать\n{sql_code}')
    response = await request_llm(prompt_)
    return response == 'OK'
