import traceback
from typing import Optional

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
import pandas as pd
from utils.timeutil import YearMonth


def run_model(messages, model_path: str) -> Optional[str]:
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        device_map="cpu",
        torch_dtype='auto').eval()
    input_ids = tokenizer.apply_chat_template(
        conversation=messages,
        tokenize=True,
        add_generation_prompt=True,
        return_tensors='pt'
    )

    try:
        output_ids = model.generate(input_ids.to('cpu'), max_length=200)
        return tokenizer.decode(output_ids[0][input_ids.shape[1]:], skip_special_tokens=True)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        del model
        del tokenizer
        torch.cuda.empty_cache()


def scale(v):
    if v > 0.9:
        return "VeryHigh"
    elif v > 0.7:
        return "High"
    elif v > 0.3:
        return "Normal"
    elif v > 0.1:
        return "Low"
    else:
        return "VeryLow"


def predict(data: pd.Series):
    factors = ["P", "val", "R_QoQ", "GP_QoQ", "O_QoQ", "EBT_QoQ", "E_QoQ", "GP/P", "EQ/P"]
    str_factors = ", ".join([f"{f}={scale(data[f])}" for f in factors])
    messages = [
        {
            "role": "user",
            "content": f"The current investment indicators are {str_factors}"
        }
    ]
    print("messages:", messages)
    predicted = run_model(messages, "./jwj-llama-ft")
    actual = scale(data["수익률_pct"])
    return predicted, actual


def main():
    hst = pd.read_csv("analysis/.cache/backup.csv", dtype={"code": str})
    hst["매수년월"] = hst["매수년월"].apply(lambda x: YearMonth.from_string(x))
    hst["매도년월"] = hst["매도년월"].apply(lambda x: YearMonth.from_string(x))
    hst = hst[hst["확정실적"].notna()]
    hst = hst[hst["매도년월"] == YearMonth(2024, 3)]
    for idx, row in hst.iterrows():
        print(predict(row))


if __name__ == '__main__':
    main()
