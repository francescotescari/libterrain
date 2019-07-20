from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Unicode, and_, ForeignKey
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape, from_shape
import numpy as np
import shapely
from geoalchemy2.functions import ST_Intersects
from sqlalchemy.orm import relationship

Base = declarative_base()


class DataUnavailable(Exception):
    pass


class Building(Base):
    __abstract__ = True
    gid = Column(Integer, primary_key=True)
    geom = Column(Geometry('POLYGON'))
    ant_height = 4
    base = Base

    def __hash__(self):
        return hash(self.gid)

    def __eq__(self, other):
        # equality performed only on gid (unique in the db)
        return self.gid == other.gid

    def __repr__(self):
        return str(self.gid)

    def shape(self):
        return to_shape(self.geom)

    def coords(self):
        return self.shape().representative_point()

    def xy(self):
        return (self.coords().x, self.coords().y)

    def coord_height(self):
        obj = {}
        obj['coords'] = self.coords()
        obj['height'] = self.ant_height
        obj['building'] = self
        return obj

    def get_height(self):
        raise NotImplementedError


class Building_CTR(Building):
    __tablename__ = 'ctr_toscana'
    foglio = Column(String)
    codice = Column(String)
    record = Column(Integer)
    topon = Column(String)
    area = Column(Float)
    identif = Column(String)

    def __str__(self):
        return "Building ID: {0} \nLongitude: {1} \nLatitude: {2} \nCodice: {3}" \
            .format(self.gid, self.coords().x, self.coords().y, self.codice)

    def get_height(self):
        raise DataUnavailable("CTR height data unavailable")


class Building_OSM(Building):
    __tablename__ = 'osm_centro'
    osm_id = Column(Integer)
    code = Column(Integer)
    fclass = Column(String)
    name = Column(Unicode)
    t_type = Column('type', String)
    height = relationship("Building_OSMHeight", backref="building", uselist=False)

    def __str__(self):
        if (self.name):
            return "Name: {3} \nBuilding ID: {0} \nLongitude: {1} \nLatitude: {2}" \
                .format(self.gid, self.coords().x, self.coords().y, self.name)
        return "Building ID: {0} \nLongitude: {1} \nLatitude: {2}" \
            .format(self.gid, self.coords().x, self.coords().y)

    def get_height(self):
        try:
            return self.height.avg_height()
        except Exception:
            raise DataUnavailable("Missing OSM height data")


class Building_OSMHeight(Base):
    __tablename__ = 'osm_building_height'
    gid = Column(Integer, ForeignKey('osm_centro.gid'), primary_key=True)
    dsm_min = Column(Float)
    dsm_avg = Column(Float)
    dsm_max = Column(Float)
    dtm_min = Column(Float)
    dtm_avg = Column(Float)
    dtm_max = Column(Float)

    def avg_height(self):
        return self.dsm_avg - self.dtm_avg
