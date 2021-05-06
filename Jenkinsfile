pipeline {
    agent any
    tools {
        maven 'Maven3'
    }

    stages {
        stage('build') {
            steps {
                echo 'Building phase'
                sh 'mvn compile'
            }
        }

        stage('test') {
             steps {
                echo 'Testing phase'
                sh 'mvn compile'
             }
        }
    }
}