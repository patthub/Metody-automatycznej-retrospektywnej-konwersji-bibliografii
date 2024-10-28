import pandas as pd
from sklearn.model_selection import train_test_split
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
import torch
from torch.utils.data import DataLoader, TensorDataset

def load_data(file_path, test_size=0.2, max_length=128, num_words=20000):
    data = pd.read_csv(file_path)
    tokenizer = Tokenizer(num_words=num_words, oov_token="<OOV>")
    tokenizer.fit_on_texts(data['Rekord'])
    
    sequences = tokenizer.texts_to_sequences(data['Rekord'])
    padded_sequences = pad_sequences(sequences, maxlen=max_length, padding='post', truncating='post')
    
    X = padded_sequences
    y = data['Etykieta'].values
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    return X_train, X_test, y_train, y_test, tokenizer.word_index
