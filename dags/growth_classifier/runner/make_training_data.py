## python scripts
import growth_classifier.training_data as training

if __name__ == "__main__":
    _ = training.make_training_data(growth_threshold=1.1, window=120)