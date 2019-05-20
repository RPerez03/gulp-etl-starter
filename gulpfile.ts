/* TypeScript: parse all .CSV files in a folder into Message Stream,
then compose them right back into CSV files again and save with
changed names */

import { src, dest } from 'gulp'
import * as rename from 'gulp-rename'
import { tapCsv } from 'gulp-etl-tap-csv'
import { targetCsv } from 'gulp-etl-target-csv'

let gulp = require('gulp');
const errorHandler = require('gulp-error-handle'); // handle all errors in one handler, but still stop the stream if there are errors
let handlelines = require('gulp-etl-handlelines');  // Required to use handleline plugin

/* Handlelines Plugin  */
    var count = 0
    const handleLine = (lineObj: any) => {
    // return null to remove this line from data
    if (!lineObj.record || !lineObj.record["carModel"]) {
        return null}
    // If line has 'carModel' then add 1 to count
    if (lineObj.record["carModel"]) {
            count = count + 1
        }
    // return the changed lineObj
    return lineObj
}

export function processCsv() {

 /* export for handlelines plugin */   
    return src(['data/*.csv', '!data/*-parsed.csv'])
    .on('data', function (file:any) {
        console.log('Starting processing on ' + file.basename)
    })  
    .pipe(tapCsv({ columns:true }))
   // Handlelines plugin call and display of Count 
    .pipe(handlelines.handlelines({}, { transformCallback: handleLine }))
    .on('data', function (file:any) {
        console.log('# of lines in ' + file.basename + ': ' + count)
    })

    // error handling 
    .pipe(errorHandler(function(err:any) {
        console.error('whoops: ' + err)
        // callback(err)
      }))  

    .pipe(rename({ extname: ".ndjson" })) // rename to *.ndjson
    .on('data', function (file:any) {
        console.log('Done parsing ' + file.basename)
    })  
    .pipe(targetCsv({header:true}))
    .pipe(rename({suffix:"-parsed", extname: ".csv" })) // rename to *.ndjson
    .on('data', function (file:any) {
        console.log('Done processing on ' + file.basename)
    })  

    // Send file to data folder
    .pipe(gulp.dest('data/'));
    }
    
