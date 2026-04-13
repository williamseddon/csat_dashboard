from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

from .config import DEFAULT_MODEL, DEFAULT_REASONING
from .utils import estimate_tokens, safe_text

try:
    from openai import OpenAI
    HAS_OPENAI = True
except Exception:
    OpenAI = None
    HAS_OPENAI = False


GENERAL_INSTRUCTIONS = (
    'You are SharkNinja Review Analyst — an internal voice-of-customer AI assistant. '
    'Synthesize consumer review data into evidence-backed insights. '
    'Lead with the most important insight, quantify claims where possible, and end with a short Next Steps section.'
)


def get_api_key() -> Optional[str]:
    return os.getenv('OPENAI_API_KEY')


def make_client(api_key: Optional[str] = None):
    key = api_key or get_api_key()
    if not (HAS_OPENAI and key):
        return None
    try:
        return OpenAI(api_key=key, timeout=60, max_retries=3)
    except TypeError:
        return OpenAI(api_key=key)


def reasoning_options_for_model(model: str) -> List[str]:
    model_lower = safe_text(model).lower()
    if not model_lower.startswith('gpt-5'):
        return ['none']
    if model_lower.startswith('gpt-5.4') or model_lower in {'gpt-5-chat-latest', 'gpt-5.2', 'gpt-5.2-pro'}:
        return ['none', 'low', 'medium', 'high', 'xhigh']
    if model_lower in {'gpt-5', 'gpt-5-mini', 'gpt-5-nano'}:
        return ['minimal', 'low', 'medium', 'high']
    return ['none', 'low', 'medium', 'high']


def normalize_reasoning_effort(model: str, reasoning_effort: Optional[str]) -> Optional[str]:
    if not safe_text(model).lower().startswith('gpt-5'):
        return None
    allowed = reasoning_options_for_model(model)
    effort = safe_text(reasoning_effort).lower()
    if effort in allowed:
        return effort
    if not effort:
        return allowed[0]
    if effort == 'none' and 'minimal' in allowed:
        return 'minimal'
    if effort == 'minimal' and 'none' in allowed:
        return 'none'
    if effort == 'xhigh' and 'high' in allowed:
        return 'high'
    return allowed[0]


def chat_complete(
    client,
    *,
    model: str,
    messages: List[Dict[str, Any]],
    temperature: float = 0.0,
    max_tokens: int = 1200,
    response_format: Optional[Dict[str, Any]] = None,
    reasoning_effort: Optional[str] = DEFAULT_REASONING,
    retries: int = 3,
) -> str:
    if client is None:
        return ''
    kwargs: Dict[str, Any] = {
        'model': model,
        'messages': messages,
        'max_completion_tokens': int(max_tokens),
    }
    effort = normalize_reasoning_effort(model, reasoning_effort)
    if effort:
        kwargs['reasoning_effort'] = effort
    if response_format:
        kwargs['response_format'] = response_format
    if not safe_text(model).lower().startswith('gpt-5'):
        kwargs['temperature'] = temperature

    last_exc = None
    for attempt in range(max(1, retries)):
        try:
            resp = client.chat.completions.create(**kwargs)
            return (resp.choices[0].message.content or '').strip()
        except Exception as exc:
            last_exc = exc
            err = str(exc).lower()
            if 'max_completion_tokens' in kwargs and 'unsupported' in err:
                token_limit = kwargs.pop('max_completion_tokens')
                kwargs['max_tokens'] = token_limit
                continue
            if 'reasoning_effort' in kwargs and 'reasoning_effort' in err:
                kwargs.pop('reasoning_effort', None)
                continue
            if any(token in err for token in ['rate_limit', '429', '500', '503', 'timeout', 'overloaded']):
                time.sleep(min((2 ** attempt), 20))
                continue
            raise
    if last_exc:
        raise last_exc
    return ''


def build_ai_context(filtered_df, summary, filter_description: str, question: str, max_reviews: int = 20) -> str:
    from .analytics import compute_metrics, monthly_trend, rating_distribution, select_relevant_reviews

    metrics = compute_metrics(filtered_df)
    trend = monthly_trend(filtered_df).tail(12).to_dict(orient='records') if not filtered_df.empty else []
    rating_dist = rating_distribution(filtered_df).to_dict(orient='records') if not filtered_df.empty else []
    relevant = select_relevant_reviews(filtered_df, question, max_reviews=max_reviews)
    evidence = []
    for _, row in relevant.iterrows():
        evidence.append(
            {
                'review_id': safe_text(row.get('review_id')),
                'rating': row.get('rating'),
                'title': safe_text(row.get('title')),
                'snippet': safe_text(row.get('review_text'))[:600],
                'submission_date': safe_text(row.get('submission_date')),
                'content_locale': safe_text(row.get('content_locale')),
            }
        )
    payload = {
        'product': {'product_id': summary.product_id, 'product_url': summary.product_url},
        'scope': {'filter_description': filter_description, 'review_count': len(filtered_df)},
        'metrics': metrics,
        'rating_distribution': rating_dist,
        'monthly_trend': trend,
        'evidence': evidence,
    }
    raw = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    while estimate_tokens(raw) > 10000 and len(payload['evidence']) > 5:
        payload['evidence'] = payload['evidence'][:-2]
        raw = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    return raw


def ask_review_analyst(
    *,
    question: str,
    filtered_df,
    summary,
    filter_description: str,
    model: str = DEFAULT_MODEL,
    reasoning_effort: str = DEFAULT_REASONING,
    target_words: int = 1200,
    chat_history: Optional[List[Dict[str, str]]] = None,
    system_instructions: Optional[str] = None,
) -> str:
    client = make_client()
    if client is None:
        raise RuntimeError('No OpenAI API key configured.')
    context = build_ai_context(filtered_df, summary, filter_description, question)
    system_prompt = (system_instructions or GENERAL_INSTRUCTIONS) + '\n\n' + f'Aim for about {int(target_words)} words.'
    history = []
    for msg in list(chat_history or [])[-8:]:
        if not isinstance(msg, dict):
            continue
        role = safe_text(msg.get('role'))
        content = safe_text(msg.get('content'))
        if role in {'user', 'assistant', 'system'} and content:
            history.append({'role': role, 'content': content})
    messages = [
        {'role': 'system', 'content': system_prompt},
        *history,
        {'role': 'user', 'content': f'User request:\n{question}\n\nReview dataset context (JSON):\n{context}'},
    ]
    return chat_complete(
        client,
        model=model,
        messages=messages,
        temperature=0.0,
        max_tokens=max(900, min(7000, int(target_words * 2.4))),
        reasoning_effort=reasoning_effort,
    )
