# Bibliographic record type classification with CNN

## Project Overview

This project implements a Convolutional Neural Network (CNN) for bibliographic record classification purposes. It uses PyTorch for model training and evaluation, with data preprocessing steps involving pandas, scikit-learn, and Keras.

## Model Performance

Below is the performance of the CNN model on the test dataset:

| Metric    | Value  |
|-----------|--------|
| Precision | 0.9056 |
| Recall    | 0.8889 |
| F1 Score  | 0.8836 |


## Installation

To set up your environment to run the code, follow these steps:

1. **Clone the repository**

```bash

git clone https://yourprojectrepository.git
cd yourprojectdirectory
```


2. **Create a virtual environment** (optional, but recommended)

```bash
python -m venv venv
source venv/bin/activate # On Windows use venv\Scripts\activate
```


3. **Install dependencies**

```bash
pip install -r requirements.txt
```


## Usage

To train the model with your data, ensure your dataset is in a CSV format with the required columns (`Rekord` for text and `Etykiety` for labels). 
If you want to use pretrained model over your dataset, use the code below.


```python
import pandas as pd
from model import TextCNN  
from utils import predict_text_class_with_labels, load_model, load_tokenizer

def apply_model_to_dataframe(file_path, model_path, tokenizer_path):
    df = pd.read_csv(file_path)
    model = load_model(TextCNN(vocab_size, embed_dim, num_classes, kernel_sizes, num_filters), model_path)  
    tokenizer = load_tokenizer(tokenizer_path)  
    
    df['predicted_class'] = predict_text_class_with_labels(df['text_column'].tolist(), model, tokenizer)
    df.to_csv('path_to_save_predictions.csv', index=False)

if __name__ == "__main__":
    file_path = 'data/our_dataset.csv'  
    model_path = 'models/saved_models/your_model.pth' 
    tokenizer_path = 'models/tokenizers/your_tokenizer.pickle' 
    apply_model_to_dataframe(file_path, model_path, tokenizer_path)

```
### This code is part of a PhD project in digital humanities 
