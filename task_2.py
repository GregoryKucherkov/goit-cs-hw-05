from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter
from matplotlib import pyplot as plt
import requests
import string


def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # check for errors HTTP
        return response.text
    except requests.RequestException as e:
        return None
    
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))

def map_function(word):
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

# MapReduce execution
def map_reduce(text):
    text = remove_punctuation(text)
    words = text.split()

    # Parallel Mapping
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Step_2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Parallel Reduce
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)

# Visualization
def visualize_top_words(result):
    top_15 = Counter(result).most_common(15)
    labeles, values = zip(*top_15)
    plt.figure(figsize=(10, 5))
    plt.barh(labeles, values, color='blue')
    plt.xlabel('Number')
    plt.ylabel('Word')
    plt.title('Top 15 words')
    plt.show()


if __name__ == '__main__':
    # Text
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:

        # MapReduce execution 
        result = map_reduce(text)

        visualize_top_words(result)
    else:
        print("Error: Couldn't get the text.")

