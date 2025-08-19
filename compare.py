import difflib
import base64
import os
from typing import Optional
from dotenv import load_dotenv
from io import BytesIO
from PyPDF2 import PdfReader
from sentence_transformers import SentenceTransformer, util

class FileCompare():

    def __init__(
        self,
        model_file_path: str,
        use_embeddings: bool = False,
        model_name: str = "all-MiniLM-L6-v2"
    ):
        self.model_file_path = model_file_path
        self.file_in_bytes = self.load_file_or_text(
            data = self.model_file_path,
            from_file=True, 
            decode_base64=True
            )
        if use_embeddings:
            self.model = SentenceTransformer(model_name)
        else:
            self.model = None


    def load_file_or_text(self, data, from_file=True, decode_base64=False):
        """
        Loads bytes from a file path or from text directly.
        - If from_file=True, `data` is treated as a file path.
        - If from_file=False, `data` is treated as text (string).
        - If decode_base64=True, `data` is assumed to be base64-encoded.
        """
        if from_file:
            with open(data, "rb") as f:
                return f.read()
        else:
            if decode_base64:
                # Expect base64 text, must be str
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                return base64.b64decode(data, validate=False)
            else:
                # If it's already bytes (like response.content), return as-is
                if isinstance(data, bytes):
                    return data
                return data.encode("utf-8")
            
    def pdf_to_text(self, file_bytes):
        reader = PdfReader(BytesIO(file_bytes))
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text
    

    def similarity_text(self, a_bytes, b_bytes):
        text_a = self.pdf_to_text(a_bytes)
        text_b = self.pdf_to_text(b_bytes)
        # return difflib.SequenceMatcher(None, text_a, text_b).ratio()
        return self.embedding_similarity(text_a, text_b)
    

    def token_jaccard_similarity(self, text1, text2):
        tokens1 = set(text1.split())
        tokens2 = set(text2.split())
        return len(tokens1 & tokens2) / len(tokens1 | tokens2)
    
    def embedding_similarity(self, text1, text2):
        
        embedding1 = self.model.encode(text1, convert_to_tensor=True)
        embedding2 = self.model.encode(text2, convert_to_tensor=True)

        similarity = util.cos_sim(embedding1, embedding2)
        return similarity.item()


    def similarity(self, a_bytes, b_bytes, sample_size=5000):
        # Take only a sample (fast)
        a_sample = a_bytes[:sample_size]
        b_sample = b_bytes[:sample_size]

        # Convert to text for difflib
        a_text = a_sample.decode(errors="ignore")
        b_text = b_sample.decode(errors="ignore")

        return difflib.SequenceMatcher(None, a_text, b_text).ratio()
    
    def compare_to_example(self, bytes, sample_size=5000):
        ratio = self.similarity_text(self.file_in_bytes, bytes)
        print(f"Similarity ratio: {ratio}")
        return ratio
    

_compare_instance: Optional[FileCompare] = None

def get_instance() -> FileCompare:

    global _compare_instance

    if _compare_instance is None:
        
        load_dotenv()
        file = os.getenv("EXAMPLE_FILE")
        model_name = os.getenv("MODEL_NAME")
        
        _compare_instance = FileCompare(file, use_embeddings=True, model_name=model_name)

    return _compare_instance