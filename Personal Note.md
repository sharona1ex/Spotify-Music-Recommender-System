# Spotify-Music-Recommendation-Engine

docker run -p 8000:8000 spotify-music-recommendation-engine



# docker login and container pushing into azure
docker login musicrecommendation.azurecr.io -u <username> -p <password>
docker build -t musicrecommendation.azurecr.io/spotify-music-recommendation-engine:v2 .
docker push musicrecommendation.azurecr.io/spotify-music-recommendation-engine:v2


# after the container docker file was updated with right set installation I had to run below query
CREATE USER [spotifycontainer] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [spotifycontainer];

This grants permission to read.


# to run the code from localhost
uvicorn app.connectASQL:app --host 0.0.0.0 --port 80