from .. import app

from flask import (
    render_template, 
    request,
    redirect,
    url_for,   
)
from werkzeug.utils import secure_filename

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    print("uploading")
    if request.method == 'POST':
        if request.files['file']:
            f = request.files['file']
            app.init._load_dataset(request.files.get('file'), app.lab)
            app.init.df.to_csv(f"{app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}", index=False)
        else:
            f = request.form.get('dataset-hidden-selection')
            app.init._load_dataset(request.form.get('dataset-hidden-selection'), app.lab)

    return redirect(url_for('active_learn'))

@app.route('/', methods=["GET","POST"])    
@app.route('/load', methods=["GET","POST"])
@app.route('/load/<status>', methods=["GET","POST"])
def load_page(status=None):
    if status == "nodata":
        return render_template(
            'load.html', 
            nodata="",
            entries=app.cached_files
        )
    else:
        return render_template(
            'load.html', 
            nodata="display:none;",
            entries=app.cached_files,
        )