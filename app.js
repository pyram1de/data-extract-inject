var sourceFolder = 'E:\\temp\\joiner_extracts\\';
var targetFolder = 'E:\\temp\\processedExtract\\';
var logPath = 'E:\\temp\\processedExtract\\logpath.log';
var fs = require('fs');
var newLine = '\r\n';
var tab = '\t';
var filesInDirectory = fs.readdirSync(sourceFolder);
var MongoClient = require('mongodb').MongoClient;
var url = 'mongodb://localhost:27017/joinc';

var validFileTypes = [{
        regx : /^HR\d\d/,
        validTabs: 38
    },{
        regx : /^HRFS/,
        validTabs: 39
    }];

var checkValidFileType = function(filename){
    var returnType = undefined,
        breakException;
    try {
        validFileTypes.forEach(function(validFileType){
            if(validFileType.regx.test(filename)){
                returnType = validFileType;
                throw breakException;
            }
        });
    } catch (e) {
        if (e!== breakException) {
            throw e;
        }
    }
    return returnType;
};

var findRecords = function(collection, query, callback){
    collection.find(query).toArray(function(err, found){
        callback(err, found);
    })
};

run(function* generatorF(resume){
    var db = yield MongoClient.connect(url, resume);
    var lookupData = db.collection('hartlinkmembers');
    for(var iny = 0; iny < filesInDirectory.length; iny = iny + 1){
        var filename = filesInDirectory[iny];
        var fileType = checkValidFileType(filename);
        if(!fileType){
            continue;
        }
        var fileText = fs.readFileSync(sourceFolder+filename,'utf8');
        var fileLines = fileText.split(newLine);
        var headerLine = fileLines.shift(); // remove the headerline
        
        fs.appendFileSync(targetFolder+filename,headerLine+newLine);
        for(var inx = 0;inx < fileLines.length;inx=inx+1){
            var fileRow  = fileLines[inx];
            var rowData = fileRow.split(tab);
            if(rowData.length!==fileType.validTabs){
                if(rowData.length>1){
                    console.log('incorrect number of tabs');
                    console.log(fileRow);
                }
                continue;
            }
            /*
                this is the bit where we need to connect to the 
                mongo database and reference the data.
            */
            
            var data = yield findRecords(lookupData,{
                "Nino" : rowData[6].substr(0,8) // first 8 characters
            }, resume);
            var prevData = {
                Surname : rowData[4],
                DOB: rowData[7],
                "First name": rowData[5]
            };
            var status;
            if(data&&data.length>0){
                status = 'matched on nino';
                rowData[4] = data[0].Surname;
                rowData[7] = data[0].DOB;
                rowData[5] = data[0]["First name"];
            } else {
                //data = yield lookupData.findOne({
                //    "Surname": rowData[4],
                //    "DOB" : rowData[7]
                //}, resume);
                //if(data){
                //    status = 'matched on surname dob';
                //    rowData[6] = data.Nino;
                //    rowData[4] = data.Surname;
                //} else {
                    status = 'not found';
                    data = [{
                        Surname : 'not found',
                        DOB: 'not found',
                        "First name": 'not found'
                    }];
                //}
            }
            
            var output = rowData.join(tab);
            fileLines[inx] = output;
            fs.appendFileSync(logPath,
                filename + tab +
                (inx + 2) + tab +
                status + tab + 
                rowData[6] + tab + 
                prevData["First name"] + tab + 
                data[0]["First name"] + tab +
                (status !== 'not found' && (prevData["First name"]!==data[0]["First name"]) ? 'Changed' : '') + tab +
                prevData.Surname + tab + 
                data[0].Surname + tab +
                (status !== 'not found' && (prevData.Surname!==data[0].Surname) ? 'Changed' : '') + tab +
                prevData.DOB + tab +
                data[0].DOB + tab +
                (status !== 'not found' && (prevData.DOB!==data[0].DOB) ? 'Changed' : '') + newLine
                );
            fs.appendFileSync(targetFolder+filename,output+newLine);
        }
        // fileLines
    }
    // filesInDirectory
    db.close();
    console.log('done');
});
// run generatorF

function run(generatorFunction){
    var generatorItr = generatorFunction(resume);
    function resume(err, callbackValue){
        if(err){
            console.log(err);
            generatorItr.next(null);
        } else {
            generatorItr.next(callbackValue);
        }
    }
    generatorItr.next();
};