from pymongo import MongoClient
import re
import emoji
import operator
import pandas as pd
import folium
from folium.features import DivIcon

client = MongoClient('localhost', 27017)
db = client['usa_db']
collection = db['usa_tweets_collection']

#### Part B ####
# Find the tweets that have at least one emoji in them
# and use defaultdict(or dictionary) to save the count of emoji per state and state per emoji.
print("\n#### Part B ####")

state_dict = dict()
# {state: [<emojis_count_per_state>]}
# where [<emojis_count_per_state>].size = emoji.UNICODE_EMOJI.size

'''
countNoneUSA = 0
countErr = 0
for tweet in tweets_iterator:
    try:
        if tweet['place']['country_code'] != 'US':
            print(str(tweet['place']['country_code']))
            countNoneUSA += 1
    except TypeError:
        countErr += 1
        pass
print(countNoneUSA)
print(countErr)
'''

NUM_EMOJIS = len(emoji.UNICODE_EMOJI)


# return a list of emojis contained in the str
def extract_emojis(str):
    return ''.join(c for c in str if c in emoji.UNICODE_EMOJI)


# return a list of counts in the same order as emojis in emoji.UNICODE_EMOJI
def count_emojis(emoji_list):
    retlist = [0] * NUM_EMOJIS
    for i, c in enumerate(emoji.UNICODE_EMOJI):
        if c in emoji_list:
            retlist[i] += 1
    return retlist


# return a list of top n emojis in the given state(2 letter abbr)
def top_emoji_per_state(stateAbbr, n):
    cnt = state_dict[stateAbbr]
    topN = sorted(zip(cnt, emoji.EMOJI_UNICODE.values()), reverse=True)[:n]
    return topN


# print stat_dict for checking
def print_stat_dict():
    for k, v in state_dict.items():
        print(k, sum(v))


tweets_iterator = collection.find()
for tweet in tweets_iterator:
    try:
        if tweet['place']['country_code'] == 'US':
            state = tweet['place']['full_name'][-2:]
            cntlist = count_emojis(extract_emojis(tweet['text']))
            if state in state_dict.keys():
                cntlist = list(map(operator.add, cntlist, state_dict[state]))
            d = {state: cntlist}
            state_dict.update(d)
    except TypeError:
        pass

# print_stat_dict()

# B1: What are the top 15 emojis used in the entire tweets
print("B1: top 15 emojis used in the entire tweets:")
totCnt = [0] * NUM_EMOJIS
for k, v in state_dict.items():
    totCnt = list(map(operator.add, v, totCnt))
# print(sum(totCnt))
top15 = sorted(zip(totCnt, emoji.EMOJI_UNICODE.values()), reverse=True)[:15]
print(top15)
print("\n")

# B2: What are the top 5 states for the emoji 🎄?
print("B2:top 5 states for the emoji 🎄:")
for i_tree, e in enumerate(emoji.EMOJI_UNICODE.values()):
    if e == '🎄':
        break
states = []
for i_state, clist in enumerate(state_dict.values()):
    # print(i_state, clist[i_tree])
    states.append(clist[i_tree])
top5tree = sorted(zip(states, state_dict.keys()), reverse=True)[:5]
print(top5tree)
print("\n")

# B3: What are the top 5 emojis for MA?
print("B3: top 5 emojis for MA")
top5MA = top_emoji_per_state("MA", 5)
print(top5MA)
print("\n")

# B4: What are the top 5 states that use emojis?
print("B4: top 5 states using emojis")
stateTot = []
for i_state, clist in enumerate(state_dict.values()):
    stateTot.append(sum(clist))
top5state = sorted(zip(stateTot, state_dict.keys()), reverse=True)[:5]
print(top5state)
print("\n")

#### Part C ####
print("\n#### Part C ####")
# C1: What are the top 5 states that have tweets?
print("C1: top 5 states that have tweets:")
QC1_itr = collection.aggregate([
    {"$project": {"state": {"$split": ["$place.full_name", ", "]}}},
    {"$unwind": "$state"},
    {"$match": {"state": {"$regex": "^[A-Z]{2}$"}}},
    {"$group": {"_id": {"state": "$state"}, "total_qty": {"$sum": 1}}},
    {"$sort": {"total_qty": -1}},
    {"$limit": 5}
])
for item in QC1_itr:
    print(item)
print("\n")

# C2: In the state of California, what are the top 5 cities that tweet?
print("C2:top 5 cities in California that tweet:")
QC2_itr = collection.aggregate([
    {"$project": {'city': "$place.full_name"}},
    {"$match": {'city': {"$regex": "CA$"}}},
    {"$group": {'_id': {"city": "$city"}, "total_qty": {"$sum": 1}}},
    {"$sort": {'total_qty': -1}},
    {"$limit": 5}
])
for item in QC2_itr:
    print(item)
print("\n")

#### Part D ####
# create a map of all the tweets
# starting at the center if USA
print("\n#### Part D ####")
map_tweets = folium.Map(location=[39.8283, -98.5795], zoom_start=3)
itr = collection.find()
for tw in itr:
    try:
        if tw['place']['country_code'] == 'US':
            loc = tw['coordinates']['coordinates']  # presented as longLat,
            loc2 = [loc[1], loc[0]]  # convert to latLong
            folium.CircleMarker(loc2, radius=0.5, color='#3186cc', fill_color='#3186cc') \
                .add_to(map_tweets)
    except TypeError:
        pass

map_tweets.save('map.html')
print("map done")

#### Extra ####
# For part 2) B,
# create the map of USA with top 2 emojis per state
# for states tweet 0 emojis output null
print("\n#### Extra ####")

map_emojis = folium.Map(location=[39.8283, -98.5795], zoom_start=4)
store_state = []
store_emojis = []
for st in state_dict.keys():
    ls = top_emoji_per_state(st, 2)
    es = [ls[0][1], ls[1][1]]
    # print(es)
    store_state.append(st)
    store_emojis.append(es)
df_state_emojis = pd.DataFrame({'state': store_state, 'emojis': store_emojis})

geo_states = pd.read_csv("geo_states.csv")

# join the two dataframe on state abbreviation
df = df_state_emojis.join(geo_states.set_index('abv'), on='state')

for index, row in df.iterrows():
    try:
        lat = row['latitude']
        long = row['longitude']
        # use to latLong
        folium.map.Marker(
            [lat, long],
            icon=DivIcon(
                icon_size=(50, 50),
                icon_anchor=(0, 0),
                html=''.join(row['emojis'])
            )
        ).add_to(map_emojis)
    except ValueError:
        pass

map_emojis.save('map_emojis.html')
print("map_emojis done")
