from textblob import TextBlob

def get_sentiment_score(text: str) -> str:
    """
    Utility function to generate sentiment of passed text
    using textblob's sentiment method
    """
    if not isinstance(text, str):
        print("Text must be of type string: ", text)
        return

    analysis = TextBlob(text)

    if analysis.sentiment.polarity > 0:
        return "positive"
    elif analysis.sentiment.polarity == 0:
        return "neutral"
    else:
        return "negative"
