#this is the wsgi.py
from routes import app
if __name__ == "__main__":
    # app.run()
    app.run(debug=True, port=2001)

# from run import app
# if __name__ == "__main__":
#     # app.run()
#     app.run(debug=True, port=2001)
