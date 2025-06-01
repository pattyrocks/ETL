import tmdbsimple as tmdb # type: ignore
import os

tmdb.API_KEY = os.getenv('TMDBAPIKEY')

def get_movies_per_page():

    result = tmdb.Discover().movie(page=1)
    movies_list_of_lists = []

    for i in range(1,3):

        result_i = tmdb.Discover().movie(page=i)['results']
        movies_list_of_lists.append(result_i)

        i=+1

    return movies_list_of_lists

def get_info_per_page():

    info_list_of_lists = []

    for i in get_movies_per_page():

        for dict in i:
            movie_id = dict['id']
            movie_info = tmdb.Movies(movie_id).info()

            info_list_of_lists.append(movie_info)
    i=+1

    print(info_list_of_lists)

get_info_per_page()
    


    

