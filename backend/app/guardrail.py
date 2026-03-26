from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models.chat_models import BaseChatModel

GUARDRAIL_BLOCKED_MESSAGE = (
    "This system is designed to answer questions related to the provided dataset only."
)

GUARDRAIL_SYSTEM = """You are a strict classifier for a conversational interface over a Neo4j graph of SAP order-to-cash data.
The graph includes business entities such as: customers, sales orders, order items, deliveries, delivery items, invoices (billing documents), billing items, payments, journal entries, products, plants, addresses, and related relationships.

Classify the user's message:
- Reply with exactly one word: ALLOW — if the user is asking about this dataset, supply chain, logistics, billing, invoicing, deliveries, payments, customers, products, or similar business/analytics questions grounded in operational data.
- Reply with exactly one word: DENY — for general knowledge, creative writing, coding unrelated to this graph, jokes, politics, personal advice, or anything not about analyzing this business dataset.

Output only ALLOW or DENY. No punctuation or explanation."""


def is_query_allowed(llm: BaseChatModel, user_message: str) -> bool:
    response = llm.invoke(
        [
            SystemMessage(content=GUARDRAIL_SYSTEM),
            HumanMessage(content=user_message.strip()),
        ]
    )
    text = (response.content or "").strip().upper()
    if "ALLOW" in text and "DENY" not in text:
        return True
    if text.startswith("ALLOW"):
        return True
    return False
