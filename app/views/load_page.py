from .. import app

from flask import (
    render_template, 
    request,
    redirect,
    url_for,   
)
from werkzeug.utils import secure_filename

@app.route('/uploader', methods = ['POST'])
def upload_file():
    """
    action in response to submit button on "/load" page
    """

    if request.method == 'POST':

        if request.files['file']:
            
            f = request.files['file']
            
            app.logger.info(f"loading {f}")
            app.logger.info(f"saving copy to: {app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}")

            app.init._load_dataset(
                request.files.get('file'), 
                app.lab
            )
            app.init.df.to_csv(
                f"{app.config['UPLOAD_FOLDER']}/{secure_filename(f.filename)}", 
                index=False
            )
        else:
            f = request.form.get('dataset-hidden-selection')
            app.logger.info(f"loading file from cache: {f}")
            app.init._load_dataset(
                request.form.get('dataset-hidden-selection'), 
                app.lab
            )

    return redirect(url_for('learn'))

@app.route('/')    
@app.route('/load')
@app.route('/load/<status>')
def load_page(status=None):
    """
    loads the "/load" page;

    if user attempts to go to a different page without having loaded a dataset, 
    user is directed to "load/nodata", which is simply the "/load" page 
    with an alert, notifying user they need to load a dataset first
    """
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