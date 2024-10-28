import torch
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from model import TextCNN
from data_loader import load_data
from utils import evaluate_model, save_model, save_tokenizer, save_label_encoder


def train_model(file_path):
    X_train, X_test, y_train, y_test, word_index = load_data(file_path)
    
    vocab_size = len(word_index) + 1
    embed_dim = 300
    num_classes = 2 
    kernel_sizes = [3, 4, 5]
    num_filters = 100
    
    model = TextCNN(vocab_size, embed_dim, num_classes, kernel_sizes, num_filters)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    
    #Prepare dataloaders
    train_data = TensorDataset(torch.tensor(X_train, dtype=torch.long), torch.tensor(y_train, dtype=torch.long))
    test_data = TensorDataset(torch.tensor(X_test, dtype=torch.long), torch.tensor(y_test, dtype=torch.long))
    train_loader = DataLoader(train_data, batch_size=64, shuffle=True)
    test_loader = DataLoader(test_data, batch_size=64, shuffle=False)
    
    #Training
    num_epochs = 5  
    for epoch in range(num_epochs):
        model.train()
        for texts, labels in train_loader:
            optimizer.zero_grad()
            outputs = model(texts)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
        print(f'Epoch {epoch+1}, Loss: {loss.item()}')
    
    #Evaluation
    accuracy = evaluate_model(model, test_loader)
    print(f'Accuracy: {accuracy}')


model_path = 'models/saved_models/model.pth'
tokenizer_path = 'models/tokenizers/tokenizer.pickle'
label_encoder_path = 'models/label_encoders/label_encoder.pickle'

save_model(model, model_path)
save_tokenizer(tokenizer, tokenizer_path)
save_label_encoder(label_encoder, label_encoder_path)


if __name__ == "__main__":
    file_path = 'path.csv'  
    train_model(file_path)
