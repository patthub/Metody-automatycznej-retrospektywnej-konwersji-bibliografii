import torch
import pickle
from sklearn.metrics import precision_score, recall_score, f1_score
from keras.preprocessing.sequence import pad_sequences


def predict_text_class_with_labels(texts, model, tokenizer, max_length=128):

    sequences = tokenizer.texts_to_sequences(texts)
    padded_sequences = pad_sequences(sequences, maxlen=max_length, padding='post', truncating='post')
    input_tensor = torch.tensor(padded_sequences, dtype=torch.long)
    
    # Prediction
    model.eval()
    with torch.no_grad():
        outputs = model(input_tensor)
        predictions = torch.argmax(outputs, dim=1).numpy()
    labels = ['artykuł', 'książka']
    predicted_labels = [labels[pred] for pred in predictions]
    
    return predicted_labels



#Functions for model evaluation

def evaluate_model(model, data_loader):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for texts, labels in data_loader:
            outputs = model(texts)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()
    accuracy = 100 * correct / total
    return accuracy


def evaluate_metrics(model, data_loader):
    model.eval()
    true_labels = []
    pred_labels = []
    
    with torch.no_grad():
        for texts, labels in data_loader:
            outputs = model(texts)
            _, predicted = torch.max(outputs, dim=1)
            true_labels.extend(labels.cpu().numpy())
            pred_labels.extend(predicted.cpu().numpy())
    
    precision = precision_score(true_labels, pred_labels, average='weighted')
    recall = recall_score(true_labels, pred_labels, average='weighted')
    f1 = f1_score(true_labels, pred_labels, average='weighted')
    
    return precision, recall, f1

#Functions for model and other related elements saving and loading

def save_model(model, model_path):
    torch.save(model.state_dict(), model_path)

def load_model(model, model_path):
    model.load_state_dict(torch.load(model_path))
    model.eval()
    return model

def save_tokenizer(tokenizer, tokenizer_path):
    with open(tokenizer_path, 'wb') as handle:
        pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)

def load_tokenizer(tokenizer_path):
    with open(tokenizer_path, 'rb') as handle:
        tokenizer = pickle.load(handle)
    return tokenizer

def save_label_encoder(label_encoder, label_encoder_path):
    with open(label_encoder_path, 'wb') as handle:
        pickle.dump(label_encoder, handle, protocol=pickle.HIGHEST_PROTOCOL)

def load_label_encoder(label_encoder_path):
    with open(label_encoder_path, 'rb') as handle:
        label_encoder = pickle.load(handle)
    return label_encoder

