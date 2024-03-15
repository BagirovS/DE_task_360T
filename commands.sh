docker volume create postgres_vol_1

docker network create app_net

docker run --rm -d \
  --name postgres_1 \
  -e POSTGRES_PASSWORD=posrtgres_admin \
  -e POSTGRES_USER=posrtgres_admin \
  -e POSTGRES_DB=test_app \
  -v postgres_vol_1:/var/lib/postgresql/data \
  -p 5432:5432 \
  --net=app_net \
  postgres:14