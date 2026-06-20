"""Test reasoning_effort parameter across all models."""

from lib import get_client
import time

client = get_client(timeout_s=120)

models = [
    # "deepseek-v4-pro-no-thinking",
    #"deepseek-v4-pro",
    #"gpt-5.4",
    #"claude-sonnet-4.6",
    #"kimi-k2.6",
    #"gemini-3.1-pro",
    #"minimax-m2.7",
    "glm-5.2",
]

prompt = "What is 7*8+3? Just the answer."

for model in models:
    for effort in [None, "high"]:
        t0 = time.time()
        try:
            kwargs = {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
            }
            if effort:
                kwargs["reasoning_effort"] = effort
            elif "deepseek" in model:
                # do not work with litellm see https://github.com/BerriAI/litellm/pull/27102
                # kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
                pass

            resp = client.chat.completions.create(**kwargs)
            elapsed = time.time() - t0

            usage = resp.usage
            comp = usage.completion_tokens if usage else 0

            details = getattr(usage, "completion_tokens_details", None)
            reasoning = details.reasoning_tokens if details else 0

            if getattr(resp.choices[0].message, "reasoning_content", None):
                reasoning = len(resp.choices[0].message.reasoning_content) // 4

            # print(resp)

            content = (resp.choices[0].message.content or "").strip()[:80]
            finish = resp.choices[0].finish_reason

            tok = f"comp={comp}"
            if reasoning:
                tok += f" reason={reasoning}"

            print(f"{model:22s} effort={effort or 'None':6s} {elapsed:4.1f}s {tok} finish={finish:6s} [{content}]")
        except Exception as e:
            print(f"{model:22s} effort={effort or 'None':6s} ERROR: {str(e)[:120]}")
    print()
