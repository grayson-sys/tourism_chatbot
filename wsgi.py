from asgiref.wsgi import ASGIToWSGI

from app.main import app

application = ASGIToWSGI(app)
