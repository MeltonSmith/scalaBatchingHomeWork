pipeline {
    agent any
        stages {
            stage('build') {
            def mvn_version = 'Maven3'
                    withEnv( ["PATH+MAVEN=${tool mvn_version}/bin"] ) {
                    sh 'mvn --version'
                    }


//                 steps {
//                     sh 'mvn --version'
//                 }
            }
        }
}