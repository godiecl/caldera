# Caldera: apliCación mAchine Learning preDicción rEmociones Rio mAipo

## Configuración del entorno vía conda

```bash
conda create -n caldera
conda activate caldera
conda config --set report_errors false
conda config --env --add channels conda-forge
conda config --env --set channel_priority strict
conda install python=3 geopandas
conda install pygeos geopy pyogrio matplotlib rasterio dask-geopandas
```
