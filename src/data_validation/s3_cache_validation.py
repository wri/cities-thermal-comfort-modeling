from city_metrix import GeoExtent
from city_metrix.cache_manager import is_cache_usable
from city_metrix.constants import CTCM_PADDED_AOI_BUFFER, GeoType
from city_metrix.layers import FabDEM, OpenUrban, OvertureBuildingsDSM, AlbedoCloudMasked, TreeCanopyHeightCTCM, BuAirTemperature

from city_metrix.metrix_tools import is_openurban_available_for_city
from src.constants import S3_PUBLICATION_BUCKET, S3_PUBLICATION_ENV

def check_city_data_availability(city_geoextent: GeoExtent):
    invalids = []

    if city_geoextent is not None and city_geoextent.geo_type == GeoType.CITY_AREA:
        city_id = city_geoextent.city_id
        is_open_urban_available = is_openurban_available_for_city(city_id)
        if not is_open_urban_available:
            msg = (f"OpenUrban dataset is not available for {city_id}")
            return (msg, True)

        # FabDEM
        invalid = _check_layer_availability(FabDEM(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # OvertureBuildingsDSM
        invalid = _check_layer_availability(OvertureBuildingsDSM(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # OpenUrban
        invalid = _check_layer_availability(OpenUrban(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # TreeCanopyHeight
        invalid = _check_layer_availability(TreeCanopyHeightCTCM(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # AlbedoCloudMasked
        invalid = _check_layer_availability(AlbedoCloudMasked(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

        # BuAirTemperature
        invalid = _check_layer_availability(BuAirTemperature(), city_geoextent)
        if invalid is not None: invalids.append(invalid)

    return invalids

def _check_layer_availability(layer_object, city_geoextent):
    layer_has_usable_cache = is_cache_usable(S3_PUBLICATION_BUCKET, S3_PUBLICATION_ENV, layer_object, city_geoextent,
                                                     aoi_buffer_m=CTCM_PADDED_AOI_BUFFER, city_aoi_subarea=None)

    if not layer_has_usable_cache:
        msg = (f"Cache not found for {city_geoextent.city_id}, {city_geoextent.aoi_id}, {layer_object.__class__.__name__}, "
               f"buffer:{CTCM_PADDED_AOI_BUFFER} in {S3_PUBLICATION_BUCKET} {S3_PUBLICATION_ENV}.")
        return (msg, True)
    else:
        return None
