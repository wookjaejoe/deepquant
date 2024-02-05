import re


def to_numeric(text: str):
    # 정규 표현식을 사용하여 숫자만 추출
    # 이 정규 표현식은 숫자, 소수점, 음수 부호를 포함한 문자열을 찾습니다.
    # 괄호, 콤마, 공백 등은 무시합니다.
    pattern = r"[-+]?\d*\.?\d+"
    matches = re.findall(pattern, text.replace(",", ""))  # 콤마는 미리 제거

    if not matches:
        return None  # 숫자가 없는 경우 None 반환

    # 첫 번째 매칭된 숫자만 사용 (복수의 숫자가 있는 경우 첫 번째 숫자를 사용한다고 가정)
    number_str = matches[0]

    # 소수점이 있는지 확인하여 float 또는 int로 변환
    if '.' in number_str:
        return float(number_str)
    else:
        return int(number_str)


if __name__ == '__main__':
    strings = ["1234", "$1,234.50", "(1,200)", "[2,345,678]", " -1234 ", "abc 123"]
    for s in strings:
        print(f"{s} -> {to_numeric(s)}")
