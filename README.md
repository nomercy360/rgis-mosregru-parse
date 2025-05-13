### Выгрузить список из всех ПЗЗ, у которых есть geometry data:

```shell
python scraper.py --max-pages 309 --workers 4

```

### Выгрузить данные о полигонах
```shell
python3 geometry_fetcher.py --input data.json --output geometries.json --concurrent 10

```