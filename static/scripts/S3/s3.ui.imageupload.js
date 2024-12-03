/**
 * Used by ImageUploadWidget (core/ui/widgets)
 */
(function($, undefined) {

    "use strict";
    var imageUploadID = 0;

    const IMAGE_EXTENSIONS = ['png', 'PNG', 'jpg', 'JPG', 'jpeg', 'JPEG'],
          ACCEPTED_FORMATS = 'png, jpeg or jpg';

    /**
     * imageUpload
     */
    $.widget('s3.imageUpload', {

        /**
         * Default options
         *
         * TODO document options
         */
        options: {
            field: "None"
        },

        /**
         * Create the widget
         */
        _create: function() {

            this.id = imageUploadID;
            imageUploadID += 1;

            this.eventNamespace = '.imageUpload';
        },

        /**
         * Update the widget options
         */
        _init: function() {

            const opts = this.options,
                  $el = $(this.element),
                  container = $el.closest('div.image-input');

            // TODO proper naming and sorting of objects
            this.canvas = $('.image-input-canvas', container);
            this.canvasSize = [0, 0];
            this.toScale = true;

            this.uploadTitle = $('.upload-title', container);
            this.uploadContainer = $('.upload-container', container);
            this.uploadedImage = $('.uploaded-image', container);

            this.dropArea = $('.imagecrop-drag', container);

            this.captureButton = $('.capture-btn', container);

            this.imageCropData = $('input[name="' + opts.field + '-imagecrop-data"]', container);

            this.selectCrop = $('.select-crop-btn', container);
            this.crop = $('.crop-btn', container);
            this.cancel = $('.remove-btn', container);

            this.image = null;
            this.fileName = null;
            this.extension = null;

            this.jCropAPI = null;
            this.cropPoints = null;

            this.refresh();
        },

        /**
         * Remove generated elements & reset other changes
         */
        _destroy: function() {

            // TODO destroy jCropAPI
            // TODO destroy cameraDialog

        },

        /**
         * Redraw contents
         */
        refresh: function() {

            this._unbindEvents();

            this.image = new Image();
            this.fileName = 'upload.png';
            this.extension = 'png';

            this._removeCropAPI();

            const $el = $(this.element),
                  container = $el.closest('div.image-input'),
                  canvas = this.canvas,
                  self = this;

            if (!canvas.length) {
                return;
            }

            var widthLimit = canvas.width(),
                heightLimit = canvas.height();

            this.toScale = true;
            if (widthLimit == 0 || heightLimit == 0) {
                this.toScale = false;
                widthLimit = heightLimit = 600;
            }
            this.canvasSize = [widthLimit, heightLimit];

            // Image already stored in DB ( Update form )
            // load the Image

            var img = this.image,
                // TODO avoid double-use of imageCropData for both input and output
                imgData = this.imageCropData.attr('value'),
                fileName = this.imageCropData.data('filename');
            if (imgData !== undefined) {
                img.src = imgData;
                img.onload = function() {
                    canvas.attr({
                        width: img.width,
                        height: img.height
                    });
                    canvas[0].getContext('2d')
                             .drawImage(img, 0, 0, img.width, img.height);

                    var t = imgData.split('.'),
                        extension = t[t.length - 1];
                    if (extension == 'jpg') {
                        extension = 'jpeg';
                    }
                    self.extension = extension;

                    // Retain the original file name if available
                    if (fileName) {
                        self.fileName = fileName;
                    } else {
                        let now = new Date()
                        self.fileName = 'upload_' + now.valueOf() + '.' + extension;
                    }

                    var data = canvas[0].toDataURL('image/' + extension);
                    self._loadImage(data);
                };
            }

            this._bindEvents();
        },

        // Preview ============================================================

        /**
         * Calculates the scaled size of the image
         *
         * @param {Array} canvasSize: the canvas size [width, height]
         * @param {Array} imageSize: the current image size [width, height]
         *
         * @returns: the resulting image size [width, height]
         */
        _calculateScale: function(canvasSize, imageSize) {

            let scaleFactor = 1.0,
                scaledSize = imageSize;

            if (scaledSize[0] > canvasSize[0]) {
                scaleFactor = canvasSize[0] / scaledSize[0];
                scaledSize[0] *= scaleFactor;
                scaledSize[1] *= scaleFactor;
            }

            if (scaledSize[1] > canvasSize[1]) {
                scaleFactor = canvasSize[1] / scaledSize[1];
                scaledSize[0] *= scaleFactor;
                scaledSize[1] *= scaleFactor;
            }

            scaledSize[0] = Math.floor(scaledSize[0]);
            scaledSize[1] = Math.floor(scaledSize[1]);

            return scaledSize;
        },

        /**
         * Loads an image into the cropping area
         *
         * @param {string} data: the image as dataURL
         */
        _loadImage: function(data) {
            // TODO cleanup

            // TODO make sure we do have a file name

            const self = this;

            let image = this.image,
                scaledImage;

            // Image uploaded by user
            image.src = data;
            image.onload = function() {

                var canvasSize = self.canvasSize, // the original canvas size
                    imageSize = [image.width, image.height];

                let scale = self._calculateScale(canvasSize, imageSize);

                let canvas = self.canvas[0];
                canvas.width = scale[0];
                canvas.height = scale[1];
                canvas.getContext('2d')
                      .drawImage(image, 0, 0, scale[0], scale[1]);

                scaledImage = canvas.toDataURL('image/' + self.extension);
                if (self.toScale) {
                    self.imageCropData.val(self.fileName + ';' + scaledImage);
                }
                else {
                    // Don't Scale
                    self.imageCropData.val(self.fileName + ';' + data);
                }
                self.uploadedImage.attr({
                    src: scaledImage,
                    style: 'display: block'
                });
                self.selectCrop.css({
                    display: 'inline'
                });
                $('hr').attr({ // TODO restrict to container resp. the particular separator
                    style: 'display:block'
                });
            };
        },

        // Cropping ===========================================================

        /**
         * Initializes the crop function
         */
        _initCropAPI: function() {
            // TODO cleanup

            const self = this;

            this._removeCropAPI();

            this.uploadedImage.Jcrop({
                //onChange: updateCropPoints,
                onChange: function(coords) {
                    self.cropPoints = [coords.x, coords.y, coords.x2, coords.y2];
                },
                opacity: 0.2,
                bgFade: true,
                bgColor: 'black',
                addClass: 'jcrop-light'
            }, function() {
                self.jCropAPI = this;
                this.ui.selection.addClass('jcrop-selection');
                this.disable();
            });
        },

        /**
         * Detaches the jCropAPI
         */
        _removeCropAPI: function() {

            this._deactivateCrop();

            const jCropAPI = this.jCropAPI;
            if (jCropAPI) {
                jCropAPI.destroy();
                this.jCropAPI = null;
            }
            this.cropPoints = null;
        },

        /**
         * Enables (activates) the crop selection
         */
        _activateCrop: function() {
            // TODO cleanup

            const jCropAPI = this.jCropAPI;

            if (!jCropAPI) {
                return;
            }

            jCropAPI.enable();

            var b = jCropAPI.getBounds(),
                midx = b[0]/2,
                midy = b[1]/2,
                addx = b[0]/4,
                addy = b[1]/4,
                defaultSelection = [midx - addx, midy - addy, midx + addx, midy + addy];

            jCropAPI.animateTo(defaultSelection);

            this.selectCrop.css({
                display: 'none'
            });
            this.crop.css({
                display: 'inline'
            });
            this.cancel.css({
                display: 'inline'
            });
        },

        /**
         * Disables (deactivates) the crop selection
         */
        _deactivateCrop: function() {
            // TODO cleanup

            // uses global vars
            // $crop, $cancel, $selectCrop

            const jCropAPI = this.jCropAPI;

            if (jCropAPI) {
                jCropAPI.release();
                jCropAPI.disable();
            }

            this.selectCrop.css({
                display: 'inline'
            });
            this.crop.css({
                display: 'none'
            });
            this.cancel.css({
                display: 'none'
            });
        },

        /**
         * Crops the image in the cropping area
         */
        _cropImage: function() {

            let image = this.image;

            // Crop the Image
            var $jcropHolder = $('.jcrop-holder', $(this.element).closest('div.image-input'));
            var width = parseInt($jcropHolder.css('width').split('px')[0]),
                height = parseInt($jcropHolder.css('height').split('px')[0]);
            var scaleX = image.width / width,
                scaleY = image.height / height;

            var coords = this.cropPoints;
            if (!coords) {
                return;
            }

            var x1 = coords[0],
                y1 = coords[1],
                x2 = coords[2],
                y2 = coords[3];

            // calculate Canvas width
            width = Math.round((x2 - x1) * scaleX);
            // calculate Canvas Height
            height = Math.round((y2 - y1) * scaleY);

            this._removeCropAPI();

            let $canvas = this.canvas,
                canvas = $canvas[0];
            $canvas.attr({
                width: width,
                height: height
            });
            canvas.getContext('2d')
                  .drawImage(image, Math.round(x1 * scaleX), Math.round(y1 * scaleX), width, height, 0, 0, width, height);

            var data = canvas.toDataURL('image/' + this.extension);
            this._loadImage(data);

        },

        // File upload ========================================================

        /**
         * Verifies wether an uploaded file is a valid image file
         *
         * @param {file} file: the uploaded file
         *
         * @returns: boolean
         */
        _isValidImage: function(file) {
            // TODO cleanup

            var info = file.type.split('/'),
                filetype = info[0],
                extension = info[info.length - 1];

            if (filetype == 'image') {
                if ($.inArray(extension, IMAGE_EXTENSIONS) != -1) {
                    if (extension == 'png' || extension == 'PNG') {
                        extension = 'png';
                    } else {
                        extension = 'jpeg';
                    }
                    return extension;
                }
            }
        },

        /**
         * Handles hovering of a file over the drop area
         *   - changes the CSS class of the target area to indicate the hovering
         *
         * @param {event} e: the event triggering the function call
         */
        _FileHoverHandler: function(e) {

            e.stopPropagation();
            e.preventDefault();

            let $target = $(e.target);

            $target.addClass('imagecrop-drag');
            if (e.type == 'dragenter') {
                $target.addClass('hover');
            } else {
                $target.removeClass('hover');
            }
        },

        /**
         * Handles file selection
         *
         * @param {event} e: the event triggering the function call
         */
        _FileSelectHandler: function(e) {
            // TODO cleanup

            const $el = $(this.element),
                  self = this;

            this._removeCropAPI();
            if (e.type == 'drop') {
                // Remove file-over style
                this._FileHoverHandler(e);
            }

            // Hide UploadContainer
            setTimeout(function() {
                self.uploadContainer.slideUp('fast', function() {
                    self.uploadTitle.html('<a>' + i18n.upload_new_image + '</a>');
                });
            }, 500);

            // Verify and load the uploaded file
            var files = e.target.files || e.originalEvent.dataTransfer.files,
                file = files[0],
                extension = this._isValidImage(file);

            if (extension) {
                this.fileName = file.name;
                this.extension = extension;

                let reader = new FileReader();
                reader.onload = function(e) {
                    self._loadImage(e.target.result);
                };
                reader.readAsDataURL(file);

            } else {

                // TODO Replace ACCEPTED_FORMATS by i18n string
                alert(i18n.invalid_image + '\n' + i18n.supported_image_formats + ': ' + ACCEPTED_FORMATS);
            }

            // Remove file selection
            // - as we will upload the dataURL instead
            $el.val('');
        },

        // Image Capture from Camera ==========================================

        /**
         * Opens the camera dialog to capture an image
         */
        _openCameraDialog: function() {

            const self = this,
                  ns = this.eventNamespace;

            // TODO move styles into theme
            // TODO fixed video width, or default to some value (read what the device can do?)?
            var captureForm = $('<div class="capture-form">').css({"overflow": "hidden", "max-width": "100%"}),
                videoInput = $('<video>').css({"width":"640px","display":"block", "margin-left":"auto","margin-right":"auto"}).appendTo(captureForm),
                // TODO i18n
                shutterButton = $('<button type="button">' + 'Capture Image' + '</button>');

            shutterButton.addClass("primary button action-btn").css({
                "display": "block",
                "width": "100%"
            }).appendTo(captureForm);


            var captureDialog = captureForm.dialog({
                title: 'Capture Image', // TODO i18n
                width: 640,
//                 height: 480,
                autoOpen: false,
                modal: true,
                'classes': {'ui-dialog': 'capture-dialog'},
                close: function() {
                }
            });

            navigator.mediaDevices.getUserMedia({
                video: {
                    facingMode: {
                        ideal: 'environment'
                    }
                },
                audio: false
            }).then((stream) => {

                    videoInput.show();
                    captureDialog.dialog('open');

                    let video = videoInput.get(0);
                    video.srcObject = stream;
                    video.play();

                    videoInput.add(shutterButton).off(ns).on('click' + ns, function(e) {
                        e.stopPropagation();
                        e.preventDefault();

                        self._captureImage(video);

                        // TODO this needs to be done in self._destroy too
                        // TODO this needs to be done before creating either, too
                        captureDialog.dialog('close').dialog('destroy');
                        captureForm.remove();
                    });
                })
                .catch((err) => {
                    alert(`An error occurred: ${err}`);
                    captureForm.dialog('destroy');
                    self.captureButton.prop('disabled', true).hide();
                });
        },

        /**
         * Takes a picture with the camera and loads it into the cropping area
         */
        _captureImage: function(video) {

            // TODO can use this.canvas here too
            let canvas = document.createElement('canvas');

            // TODO Calculate from video (height may be unavailable)
            canvas.width = video.videoWidth;
            canvas.height = video.videoHeight;

            // TODO fallback for width/height

            let context = canvas.getContext("2d");

            context.drawImage(video, 0, 0, canvas.width, canvas.height);

            let data = canvas.toDataURL("image/png");
            if (data) {
                let now = new Date();
                this.fileName = 'capture_' + now.valueOf() + '.png';
                this.extension = 'png';
                this._loadImage(data);
            }

            canvas.remove();
        },

        // Utilities ==========================================================

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            $el.on('change' + ns, function(e) {
                self._FileSelectHandler(e);
            });

            this.uploadTitle.on('click' + ns, function(e) {
                self.uploadContainer.slideDown('fast', function() {
                    self.uploadTitle.html(i18n.upload_image);
                });
            });
            this.selectCrop.on('click' + ns, function() {
                self._activateCrop();
            });
            this.crop.on('click' + ns, function() {
                self._cropImage();
            });
            this.cancel.on('click' + ns, function() {
                self._deactivateCrop();
            });
            this.uploadedImage.on('load' + ns, function() {
                self._initCropAPI();
            });


            this.dropArea.on('dragenter' + ns, function(e) {
                e.stopPropagation();
                e.preventDefault();
                self._FileHoverHandler(e);
            }).on('dragover' + ns, function(e) {
                e.stopPropagation();
                e.preventDefault();
            }).on('dragleave' + ns, function(e) {
                self._FileHoverHandler(e);
            }).on('drop' + ns, function(e) {
                e.stopPropagation();
                e.preventDefault();
                self._FileSelectHandler(e);
            });

            this.captureButton.on('click' + ns, function() {
                self._openCameraDialog();
            });

            return this;
        },

        /**
         * Unbind events (before refresh)
         */
        _unbindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace;

            $el.off(ns);

            this.uploadTitle.off(ns);
            this.selectCrop.off(ns);
            this.crop.off(ns);
            this.cancel.off(ns);

            this.uploadedImage.off(ns);

            this.dropArea.off(ns);
            this.captureButton.off(ns);

            return this;
        }
    });

})(jQuery);

 $(function () {

 });
