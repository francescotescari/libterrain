from sqlalchemy.orm import sessionmaker, mapper, relationship
from sqlalchemy import create_engine, and_
from geoalchemy2.functions import GenericFunction
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape
from libterrain.building import Building_CTR, Building_OSM, Building_OSMHeight
from libterrain.comune import Comune

class BuildingInterface():
    def __init__(self, DSN, srid, building_class, height_class=None):
        engine = create_engine(DSN, client_encoding='utf8', echo=False)
        Session = sessionmaker(bind=engine)
        self.session = Session()
        self.srid = srid
        self.building_class = building_class
        self.meta = building_class.base.metadata
        self.engine = engine
        self.height_class = height_class

    def get_province_area(self, name):
        comune = Comune.get_by_name(self.session, name.upper())
        return comune.shape()

    @classmethod
    def get_best_interface(cls, DSN, area_name):
        CTR = CTRInterface(DSN)
        area = CTR.get_province_area(area_name)
        OSM = OSMInterface(DSN)
        if CTR.count_buildings(area) > OSM.count_buildings(area):
            print("Choosed CTR")
            return CTR
        else:
            print("Choosed OSM")
            return OSM

    def add_economics_data(self, table_name, mapper_object, property_name='eco_data' ):
        def void(state=None):
            return
        self.meta.reflect(bind=self.engine, views=True, only=[table_name])
        table = self.meta.tables[table_name]
        mapper_object.__setstate__ = void
        mapper_object.__getstate__ = void
        mapper(mapper_object, table)
        self.building_class.__mapper__.add_property(property_name,
                                                    relationship(mapper_object, backref="building", uselist=False))

    def get_buildings(self, shape, area=None):
        raise NotImplementedError


class CTRInterface(BuildingInterface):
    def __init__(self, DSN, srid='4326'):
        super(CTRInterface, self).__init__(DSN, srid, Building_CTR)
        self._set_building_filter()

    def _set_building_filter(self, codici=['0201', '0202', '0203', '0211',
                                           '0212', '0215', '0216', '0223',
                                           '0224', '0225', '0226', '0227', '0228']):
        """Set the filter for the building from CTR.
        codici: set of strings representing the codici
            '0201': Civil Building
            '0202': Industrial Building
            '0203': Religion Building
            '0204': Unfinished Building
            '0206': Portico
            '0207': Baracca/Edicola
            '0208': Tettoia/Pensilina
            '0209': Tendone Pressurizzato
            '0210': Serra
            '0211': Casello / Stazione Ferroviaria
            '0212': Centrale Elettrica/Sottostazione
            '0215': Capannone Vivaio
            '0216': Stalla/ Fienile
            '0223': Complesso Ospedaliero
            '0224': Complesso Scolastico
            '0225': Complesso Sportivo
            '0226': Complesso Religioso
            '0227': Complesso Sociale
            '0228': Complesso Cimiteriale
            '0229': Campeggio/ Villaggio
        """
        self.codici = codici

    def get_buildings(self, shape, area=None):
        """Get the buildings intersecting a shape
        point: shapely object
        """
        wkb_element = from_shape(shape, srid=self.srid)
        if area:
            wkb_area = from_shape(area, srid=self.srid)
            building = self.session.query(Building_CTR) \
                .filter(Building_CTR.codice.in_(self.codici),
                        Building_CTR.geom.ST_Intersects(wkb_element),
                        Building_CTR.geom.ST_Intersects(wkb_area)) \
                .order_by(Building_CTR.gid)
        else:
            building = self.session.query(Building_CTR) \
                .filter(and_(Building_CTR.codice.in_(self.codici),
                             Building_CTR.geom.ST_Intersects(wkb_element))) \
                .order_by(Building_CTR.gid)

        return building.all()

    def count_buildings(self, shape):
        """Get the buildings intersecting a shape
        point: shapely object
        """
        wkb_element = from_shape(shape, srid=self.srid)
        building = self.session.query(Building_CTR) \
            .filter(and_(Building_CTR.codice.in_(self.codici),
                         Building_CTR.geom.ST_Intersects(wkb_element)))
        return building.count()

    def get_building_gid(self, gid):
        """Get building by gid
        gid: identifier of building
        """
        building = self.session.query(Building_CTR) \
            .filter_by(gid=gid).first()
        return building


class OSMInterface(BuildingInterface):
    def __init__(self, DSN, srid='4326'):
        super(OSMInterface, self).__init__(DSN, srid, Building_OSM, Building_OSMHeight)

    def get_buildings(self, shape, area=None):
        """Get the buildings intersecting a shape
        point: shapely object
        """
        wkb_element = from_shape(shape, srid=self.srid)
        if area:
            wkb_area = from_shape(area, srid=self.srid)
            building = self.session.query(Building_OSM) \
                .filter(and_(Building_OSM.geom.ST_Intersects(wkb_area),
                             Building_OSM.geom.ST_Intersects(wkb_element))) \
                .order_by(Building_OSM.gid)
        else:
            building = self.session.query(Building_OSM) \
                .filter(Building_OSM.geom.ST_Intersects(wkb_element)) \
                .order_by(Building_OSM.gid)
        return building.all()

    def count_buildings(self, shape):
        """Get the buildings intersecting a shape
        point: shapely object
        """
        wkb_element = from_shape(shape, srid=self.srid)
        building = self.session.query(Building_OSM) \
            .filter(Building_OSM.geom.ST_Intersects(wkb_element))
        result = building.count()
        return result

    def get_building_gid(self, gid):
        """Get building by gid
        gid: identifier of building
        """
        building = self.session.query(Building_OSM) \
            .filter_by(gid=gid).first()
        return building
