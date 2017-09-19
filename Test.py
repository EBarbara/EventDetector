import csv

from nltk import pos_tag, ne_chunk
from nltk.stem import WordNetLemmatizer
from nltk.tag.stanford import StanfordPOSTagger, StanfordNERTagger
from nltk.tokenize import TweetTokenizer
from nltk.tree import Tree


# def extract_hashtags(tagged_tokens):
#   return [t for t in tagged_tokens if t[1].startswith('HT')]


# def extract_terms(tagged_tokens):
#    return [(lemmatizer.lemmatize(t[0]), t[1]) for t in tagged_tokens if
#            t[1].startswith('NN') or t[1].startswith('VB') or t[1].startswith('JJ')]


# def extract_entities(named_terms):
#    return [t for t in named_terms if t[1] != "O"]


def save(tweet, entities):
    print("saving tweet id {}".format(tweet["id"]))
    with open("tweets/tweet_filtered_nltk.csv", 'a', encoding='utf-8') as csv_file:
        field_names = ['id', 'time', 'place', 'latitude', 'longitude', 'text', 'entities']
        writer = csv.DictWriter(csv_file, delimiter=';', lineterminator='\n', fieldnames=field_names)
        writer.writerow({'id': tweet["id"],
                         'time': tweet["time"],
                         'place': tweet["place"],
                         'latitude': tweet["latitude"],
                         'longitude': tweet["longitude"],
                         'text': tweet["text"],
                         'entities': entities})


def get_entities(named):
    entities = []
    for detected in named:
        if type(detected) == Tree:
            entity = detected.label(), " ".join([token for token, pos in detected.leaves()])
            entities.append(entity)
    return entities


if __name__ == "__main__":
    tweets = []
    tokenizer = TweetTokenizer(strip_handles=True, reduce_len=True)
    # lemmatizer = WordNetLemmatizer()
    # postagger = StanfordPOSTagger('util/gate-EN-twitter.model', 'util/stanford-postagger.jar', encoding='utf8',
    #                               verbose=True, java_options='-mx2048m')
    # nertagger = StanfordNERTagger('util/english.conll.4class.distsim.crf.ser.gz', 'util/stanford-ner.jar',
    #                              encoding='utf8', verbose=True, java_options='-mx2048m')

    with open('tweets/tweet.csv', encoding='utf-8') as tweet_file:
        field_names = ['id', 'time', 'place', 'latitude', 'longitude', 'text']
        raw_tweets = csv.reader(tweet_file, delimiter=';', lineterminator='\n')
        for row in raw_tweets:
            tweet = {"id": row[0],
                     "time": row[1],
                     "place": row[2],
                     "latitude": row[3],
                     "longitude": row[4],
                     "text": row[5]}
            tweets.append(tweet)

    total_count = 0
    filtered_count = 0

    for tweet in tweets:
        tokens = tokenizer.tokenize(tweet["text"])
        tagged = pos_tag(tokens)
        named = ne_chunk(tagged)
        entities = get_entities(named)
        total_count += 1
        if len(entities) > 0:
            save(tweet, entities)
            filtered_count += 1
            percentage = (filtered_count * 100)/total_count
            print("Saving {} from {} processed tweets. Percentage = {}%".
                  format(filtered_count, total_count, percentage))

        # POS_tagged = postagger.tag(tokens)
        # hashtags = extract_hashtags(POS_tagged)
        # terms = extract_terms(POS_tagged)
        # named_terms = nertagger.tag(tokens)
        # entities = extract_entities(named_terms)
        # if len(entities) > 0:
        #    save(tweet, entities)


