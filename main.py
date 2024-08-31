import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
movies_df = pd.read_csv('movies.csv')
ratings_df = pd.read_csv('ratings.csv')
tags_df = pd.read_csv('tags.csv')
links_df = pd.read_csv('links.csv')

# Step 1: Group ratings by movieId and calculate count and mean
movie_ratings = ratings_df.groupby('movieId').agg({'rating': ['count', 'mean']})
movie_ratings.columns = ['rating_count', 'rating_mean']

# Step 2: Join with movies dataframe
popular_movies_df = movie_ratings.join(movies_df.set_index('movieId'), on='movieId')

# Step 3: Filter movies with more than 50 ratings
popular_movies_df = popular_movies_df[popular_movies_df['rating_count'] > 50]

# 4. Find the movie with the maximum number of user ratings
most_rated_movie_id = popular_movies_df['rating_count'].idxmax()
most_rated_movie_title = popular_movies_df.loc[most_rated_movie_id, 'title']
print(f"The movie with the maximum number of user ratings is: {most_rated_movie_title}")

# 5. Find all correct tags for "Matrix, The (1999)"
matrix_id = movies_df[movies_df['title'] == 'Matrix, The (1999)']['movieId'].values[0]
tags_for_matrix = tags_df[tags_df['movieId'] == matrix_id]['tag'].unique()
print("Tags for Matrix, The (1999):", tags_for_matrix)

# 6. Calculate the average user rating for "Terminator 2: Judgment Day (1991)"
terminator_id = movies_df[movies_df['title'] == 'Terminator 2: Judgment Day (1991)']['movieId'].values[0]
average_rating = ratings_df[ratings_df['movieId'] == terminator_id]['rating'].mean()
print(f"Average rating for Terminator 2: Judgment Day (1991): {average_rating}")

# 7. Data distribution of user ratings for "Fight Club (1999)"
fight_club_id = movies_df[movies_df['title'] == 'Fight Club (1999)']['movieId'].values[0]
ratings_fight_club = ratings_df[ratings_df['movieId'] == fight_club_id]['rating']

plt.figure(figsize=(10, 6))
sns.histplot(ratings_fight_club, kde=True)
plt.title('Distribution of Ratings for Fight Club (1999)')
plt.xlabel('Rating')
plt.ylabel('Frequency')
plt.show()

# 8. Find the most popular movie based on average user ratings
most_popular_movie = popular_movies_df.loc[popular_movies_df['rating_mean'].idxmax()]
print(f"The most popular movie based on average user ratings is: {most_popular_movie['title']}")

# 9. Select top 5 popular movies based on number of user ratings
top_5_movies = popular_movies_df.sort_values(by='rating_count', ascending=False).head(5)
print("Top 5 popular movies based on number of user ratings:")
print(top_5_movies['title'].tolist())

# 10. Find the third most popular Sci-Fi movie based on the number of user ratings
sci_fi_movies = popular_movies_df[popular_movies_df['genres'].str.contains('Sci-Fi', na=False)]
if not sci_fi_movies.empty:
    third_most_popular_sci_fi = sci_fi_movies.sort_values(by='rating_count', ascending=False).iloc[2]
    print(f"The third most popular Sci-Fi movie based on number of user ratings is: {third_most_popular_sci_fi['title']}")
else:
    print("No Sci-Fi movies found.")

# 11. Scrape IMDb ratings for movies with more than 50 ratings
def fetch_imdb_rating(imdb_id):
    try:
        imdb_url = f"https://www.imdb.com/title/tt{str(imdb_id).zfill(7)}/"
        print(f"Fetching IMDb rating from: {imdb_url}")  # Debugging line
        response = requests.get(imdb_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        rating = soup.find('span', itemprop='ratingValue')
        return float(rating.text) if rating else None
    except Exception as e:
        print(f"Error fetching IMDb rating for {imdb_id}: {e}")
        return None

# Add imdbId to the popular_movies_df
popular_movies_df = popular_movies_df.reset_index()
popular_movies_df = popular_movies_df.merge(links_df, on='movieId')

# Fetch IMDb ratings
with ThreadPoolExecutor(max_workers=10) as executor:
    imdb_ratings = list(executor.map(fetch_imdb_rating, popular_movies_df['imdbId']))

# Add IMDb ratings to the DataFrame
popular_movies_df['imdb_rating'] = imdb_ratings

# Drop rows with missing IMDb ratings
popular_movies_df.dropna(subset=['imdb_rating'], inplace=True)

# Find movie with highest IMDb rating
if not popular_movies_df.empty:
    highest_imdb_movie = popular_movies_df.loc[popular_movies_df['imdb_rating'].idxmax()]
    highest_imdb_movie_id = highest_imdb_movie['movieId']
    print(f"MovieId with the highest IMDb rating: {highest_imdb_movie_id}")

    # Find Sci-Fi movie with highest IMDb rating
    sci_fi_movies = popular_movies_df[popular_movies_df['genres'].str.contains('Sci-Fi', na=False)]
    if not sci_fi_movies.empty:
        highest_sci_fi_imdb_movie = sci_fi_movies.loc[sci_fi_movies['imdb_rating'].idxmax()]
        highest_sci_fi_imdb_movie_id = highest_sci_fi_imdb_movie['movieId']
        print(f"Sci-Fi movieId with the highest IMDb rating: {highest_sci_fi_imdb_movie_id}")
    else:
        print("No Sci-Fi movies found.")
else:
    print("No movies with IMDb ratings found.")  
