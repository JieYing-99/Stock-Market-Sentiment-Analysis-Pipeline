from transformers import BertTokenizer, BertForSequenceClassification, pipeline

class SentimentPipeline:
    def __init__(self):
        self.finbert = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone',num_labels=3)
        self.tokenizer = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone')
        self.sentiment_pipeline = pipeline('sentiment-analysis', model=self.finbert, tokenizer=self.tokenizer)
    
    def classify(self, text):
        result = self.sentiment_pipeline(text)
        return result[0]['label']