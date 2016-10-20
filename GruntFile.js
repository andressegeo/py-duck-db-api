exec = require('child_process').exec;
module.exports = function(grunt) {
  // Configuration de Grunt
  grunt.initConfig({
    "shell" : {
        "test-server" : {
          "command" : "python2 -m pytest api/"
        },
        "setup-venv" : {
            "command" : [
              "rm -rf lib/ venv/",
              "virtualenv venv",
              grunt.template.process(
                "export SDK_APP_ENGINE_PATH=<%=SDK_APP_ENGINE_PATH%>",
                {
                  "data" : {
                    "SDK_APP_ENGINE_PATH" : grunt.option('SDK_APP_ENGINE_PATH')
                  }
                }
              ),
              'echo "`pwd`/lib/" > venv/lib/python2.7/site-packages/lib.pth',
              'echo $SDK_APP_ENGINE_PATH >> venv/lib/python2.7/site-packages/lib.pth',
              'echo $SDK_APP_ENGINE_PATH/lib/yaml/lib/ >> venv/lib/python2.7/site-packages/lib.pth',
              '. venv/bin/activate ',
              'pip install -r requirements.txt -t lib/'
            ].join(' && ')
        },
        "setup-mysql-dev" : {
            "command" : [
              "echo '\n***** INSTALL DOCKER ******\n'",
              "apt-get install docker",
              "echo '\n***** INSTALL Mysql client ******\n'",
              "apt-get install mysql-client",
              "docker pull mysql",
              "echo `\n***** KILL instance if running & start new one******\n`",
              `image_id=\`docker ps | grep hours-mysql | awk '{print $1}'\`
               if [ -n "$image_id" ]
               then
                 echo "Kill the current container"
                 docker kill $image_id
                 docker rm $image_id
               fi
               docker run --name hours-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=localroot1234 -d mysql`,
              "echo '\n=> Connect to local server with mysql -h 127.0.0.1 -u root -p\n'"
            ].join(' && ')
        },
        "setup-mysql-dev-schema" : {
            "command" : 'mysql -h "127.0.0.1" -u "root" -plocalroot1234 < "database_sources/init.sql"'
        }
    }
  });
  // #
  /* Load tasks from folder task */
  grunt.loadTasks('tasks');
  grunt.loadNpmTasks('grunt-shell');
  grunt.registerTask("setup-venv", ["shell:setup-venv"]);
  grunt.registerTask("setup-mysql-dev", ["shell:setup-mysql-dev"]);
  grunt.registerTask("setup-mysql-dev-schema", ["shell:setup-mysql-dev-schema"]);
  grunt.registerTask("test-server", ["shell:test-server"]);
  grunt.registerTask("deploy", ["test-server", "build"]);
}
