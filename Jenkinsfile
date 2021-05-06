pipeline {
    agent any
    tools {
        maven 'Maven3'
    }


    stages {
        stage('build') {
            steps {
                sh 'mvn --version'
                sh 'mvn package'
            }
        }
    }
}