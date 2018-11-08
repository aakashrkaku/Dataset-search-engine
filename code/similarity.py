import pickle
import numpy as np
loaded_embeddings=pickle.load(open('loaded_embedding','rb'))

words=pickle.load(open('words','rb'))

idx2words=pickle.load(open('idx2words','rb'))

def cos_sim(b,A,epsilon = 1e-5):
    """Takes 2 vectors a, b and returns the cosine similarity according 
    to the definition of the dot product
    """
    dot_product = np.dot(A, b)
    norm_a = np.linalg.norm(A,axis=1) + epsilon
    norm_b = np.linalg.norm(b) + epsilon
    return dot_product / (norm_a * norm_b)


def find_nearest(ref_vec, words, embedding,topk=5):
    """
    Finds the top-k most similar words to "word" in terms of cosine similarity in the given embedding
    :param ref_vec: reference word vector
    :param words: dict, word to its index in the embedding
    :param embedding: numpy array of shape [V, embedding_dim]
    :param topk: number of top candidates to return
    :return a list of top-k most similar words
    """
    # compute cosine similarities
    scored_words = cos_sim(ref_vec, loaded_embeddings)
    
    # sort the words by similarity and return the topk
    sorted_words = np.argsort(-scored_words)
    
    return [(idx2words[w]) for w in sorted_words[:topk]]
