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
         */
        options: {

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
                  widgetID = $el.attr('id'),
                  container = $el.closest('div.image-upload');

            // TODO proper naming and sorting of objects
            this.canvas = $('.image-upload-canvas', container);
            this.canvasSize = [0, 0];
            this.toScale = true;

            this.uploadTitle = $('.upload-title', container);
            this.uploadContainer = $('.upload-container', container);
            this.uploadedImage = $('.uploaded-image', container);

            this.dropArea = $('.imagecrop-drag', container);

            this.captureButton = $('.capture-btn', container);
            this.videoDialog = null;

            this.imageData = $('input#' + widgetID + '-imagecrop-data', container);

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

            this._removeCropAPI();
            this._removeVideoDialog();
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
            this._removeVideoDialog();

            const $el = $(this.element),
                  container = $el.closest('div.image-upload'),
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

            // Load previously uploaded image
            var img = this.image,
                imgData = this.imageData,
                downloadURL = imgData.data('url');

            if (downloadURL) {
                img.src = downloadURL;
                img.onload = function() {
                    // Draw image on canvas
                    canvas.attr({
                        width: img.width,
                        height: img.height
                    });
                    canvas[0].getContext('2d')
                             .drawImage(img, 0, 0, img.width, img.height);

                    // Determine file type/extension
                    let t = downloadURL.split('.'),
                        extension = t[t.length - 1];
                    if (extension == 'jpg') {
                        extension = 'jpeg';
                    }
                    self.extension = extension;

                    // Retain the original file name if available
                    let fileName = imgData.data('filename');
                    if (fileName) {
                        self.fileName = fileName;
                    } else {
                        let now = new Date();
                        self.fileName = 'upload_' + now.valueOf() + '.' + extension;
                    }

                    // Re-write canvas data to image, and load into preview/crop area
                    let data = canvas[0].toDataURL('image/' + extension, 0.99);
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
            const self = this,
                  image = this.image;

            // Image uploaded by user
            image.src = data;
            image.onload = function() {

                var canvasSize = self.canvasSize, // the original canvas size
                    imageSize = [image.width, image.height];

                let scale = self._calculateScale(canvasSize, imageSize),
                    canvas = self.canvas[0];

                canvas.width = scale[0];
                canvas.height = scale[1];

                canvas.getContext('2d')
                      .drawImage(image, 0, 0, scale[0], scale[1]);

                let scaledImage = canvas.toDataURL('image/' + self.extension, 0.99);
                if (self.toScale) {
                    self.imageData.val(self.fileName + ';' + scaledImage);
                }
                else {
                    // Don't Scale
                    self.imageData.val(self.fileName + ';' + data);
                }
                self.uploadedImage.attr({
                    src: scaledImage,
                    style: 'display: block'
                });
                self.selectCrop.css({
                    display: 'inline'
                }).siblings('hr').show();
            };
        },

        // Cropping ===========================================================

        /**
         * Initializes the crop function
         */
        _initCropAPI: function() {

            const self = this;

            this._removeCropAPI();

            this.uploadedImage.Jcrop({
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

            const jCropAPI = this.jCropAPI;
            if (!jCropAPI) {
                return;
            }

            jCropAPI.enable();

            var b = jCropAPI.getBounds(),
                dx = b[0]/7,
                dy = b[1]/7,
                defaultSelection = [dx, dy, b[0] - dx, b[1] - dy];

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
            var $jcropHolder = $('.jcrop-holder', $(this.element).closest('div.image-upload'));
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

            this._deactivateCrop();
            this._removeCropAPI();

            let $canvas = this.canvas,
                canvas = $canvas[0];
            $canvas.attr({
                width: width,
                height: height
            });
            canvas.getContext('2d')
                  .drawImage(image, Math.round(x1 * scaleX), Math.round(y1 * scaleX), width, height, 0, 0, width, height);

            var data = canvas.toDataURL('image/' + this.extension, 0.99);
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
         * Indicates whether there is a file over the drop area (or not)
         *   - changes the CSS class of the target area
         *
         * @param {event} e: the event triggering the function call
         */
        _indicateFileOver: function(e) {

            e.stopPropagation();
            e.preventDefault();

            let dropArea = $(e.target);
            if (e.type == 'dragenter') {
                dropArea.addClass('hover');
            } else {
                dropArea.removeClass('hover');
            }
        },

        /**
         * Loads the selected file (either file input or drag&drop)
         *
         * @param {event} e: the event triggering the function call
         */
        _loadSelectedFile: function(e) {

            const $el = $(this.element),
                  self = this;

            this._removeCropAPI();
            this._indicateFileOver(e);

            // Hide UploadContainer
            this._hideUpload(true);

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
        _openVideoDialog: function() {

            const self = this,
                  ns = this.eventNamespace;

            var videoForm = $('<div class="capture-form">'),
                videoInput = $('<video class="preview-video">').appendTo(videoForm),
                shutterButton = $('<button type="button">' + i18n.capture_image_ok + '</button>');

            shutterButton.addClass("primary button action-btn shutter-btn").appendTo(videoForm);

            this.videoForm = videoForm;

            var captureButton = this.captureButton.hide(),
                throbber = $('<div class="inline-throbber">').insertAfter(captureButton);

            var videoDialog = videoForm.dialog({
                title: i18n.capture_image_from_video,
                width: 800,
                //height: 600,
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
                    },
                    width: { ideal: 1280 },
                    height: { ideal: 1280 },
                },
                audio: false
            }).then((stream) => {

                throbber.remove();
                captureButton.show();

                videoInput.show();
                videoForm.dialog('open');

                let video = videoInput.get(0);
                video.srcObject = stream;
                video.play();

                videoInput.add(shutterButton).off(ns).on('click' + ns, function(e) {
                    e.stopPropagation();
                    e.preventDefault();

                    self._captureImage(video, stream);
                    self._removeVideoDialog();
                });
            }).catch((err) => {
                alert(`An error occurred: ${err}`);

                throbber.remove();
                self.captureButton.prop('disabled', true).hide();

                self._removeVideoDialog();
            });
        },

        /**
         * Captures a still image from the video stream and loads it into the cropping area
         *
         * @param {DOMElement} video: the video DOM node
         */
        _captureImage: function(video, stream) {

            const self = this;

            var data,
                useImageCapture = false;
            if ("ImageCapture" in window) {
                try {
                    const track = stream.getVideoTracks()[0];

                    let imageCapture = new ImageCapture(track);
                    imageCapture.takePhoto().then(function(blob) {

                        data = URL.createObjectURL(blob);

                        self._hideUpload(true);
                        if (data) {
                            let now = new Date();
                            self.fileName = 'capture_' + now.valueOf() + '.png';
                            self.extension = 'png';
                            self._loadImage(data);
                        }
                    });

                    useImageCapture = True;

                } catch(e) {
                    // pass
                }
            }

            if (!useImageCapture) {
                let canvas = document.createElement('canvas');

                canvas.width = video.videoWidth;
                canvas.height = video.videoHeight;
                canvas.getContext("2d")
                      .drawImage(video, 0, 0, canvas.width, canvas.height);

                data = canvas.toDataURL("image/png", 0.99);
                canvas.remove();

                self._hideUpload(true);
                if (data) {
                    let now = new Date();
                    self.fileName = 'capture_' + now.valueOf() + '.png';
                    self.extension = 'png';
                    self._loadImage(data);
                }
            }

        },

        /**
         * Removes the video dialog
         */
        _removeVideoDialog: function() {

            var videoForm = this.videoForm;
            if (videoForm) {
                videoForm.dialog('close').dialog('destroy').remove();
                this.videoForm = null;
            }
        },

        // Utilities ==========================================================

        /**
         * Hides the upload area after image upload/capture
         *
         * @param {bool} hasImage: whether an image has been loaded
         */
        _hideUpload: function(hasImage) {

            const self = this;

            var label = hasImage ? i18n.upload_new_image : i18n.upload_image;
            setTimeout(function() {
                self.uploadContainer.slideUp('fast', function() {
                    self.uploadTitle.html('<a>' + label + '</a>');
                });
            }, 500);
        },

        /**
         * Bind events to generated elements (after refresh)
         */
        _bindEvents: function() {

            let $el = $(this.element),
                ns = this.eventNamespace,
                self = this;

            $el.on('change' + ns, function(e) {
                self._loadSelectedFile(e);
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
                self._indicateFileOver(e);
            }).on('dragover' + ns, function(e) {
                e.stopPropagation();
                e.preventDefault();
            }).on('dragleave' + ns, function(e) {
                self._indicateFileOver(e);
            }).on('drop' + ns, function(e) {
                e.stopPropagation();
                e.preventDefault();
                self._loadSelectedFile(e);
            });

            this.captureButton.on('click' + ns, function() {
                self._openVideoDialog();
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
