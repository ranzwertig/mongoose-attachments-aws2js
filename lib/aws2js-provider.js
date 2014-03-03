// Copyright (c) 2011-2013 Firebase.co - http://www.firebase.co
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

var attachments = require('mongoose-attachments');
var s3 = require('aws2js').load('s3');
var util = require('util');

function S3Storage(options) {
  attachments.StorageProvider.call(this, options);
  s3.setCredentials( options.key, options.secret );
  s3.setBucket( options.bucket );
  this.acl = options.acl || false;
  this.client = s3;
  this.endpoint = options.endpoint || ( 'https://' + options.bucket + '.s3.amazonaws.com' );
}
util.inherits(S3Storage, attachments.StorageProvider);

S3Storage.prototype.getUrl = function( path ){
  return this.endpoint + path;
};

S3Storage.prototype.createOrReplace = function(attachment, cb) {

  var self = this;

  var getOldKeys = function(cb) {
    if(self.options.keepOldFiles && self.options.uniqueStoragePath) return cb(null, []);
    var oldKeys = [];
    var paths = attachment.path.split('/');
    var prefix = [paths[1], paths[2], paths[3]].join('/');
    var url = '?prefix=' + encodeURI(prefix);

    s3.get(url, 'xml', function (error, data) {
      if(error) return cb(error);

      for(var _key in data.Contents) {
        var key = data.Contents[_key];
        if(typeof(key.Key) !== 'undefined') {
          oldKeys.push({
            key: key.Key
          });
        }
      }
      cb(null, oldKeys);
    });
  };

  var deleteOldKeys = function(oldKeys, cb) {
    if(self.options.keepOldFiles && self.options.uniqueStoragePath) return cb(null);
    if(oldKeys.length === 0) return cb(null);
    self.client.delMultiObjects(oldKeys, cb);
  };

  getOldKeys(function(err, oldKeys) {
    self.client.putFile(attachment.path,attachment.filename, self.acl, {}, function(err, uploadRes) {
      if(err) return cb(err);
      attachment.defaultUrl = self.getUrl( attachment.path );
      deleteOldKeys(oldKeys, function(err) {
        if(err) return cb(err);
        cb(null, attachment);
      });
    });

  });

};

// register the S3 Storage Provider into the registry
attachments.registerStorageProvider('aws2js', S3Storage);

// export it just in case the user needs it
module.exports = S3Storage;
