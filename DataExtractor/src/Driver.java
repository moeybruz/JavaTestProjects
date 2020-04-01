package application;

import service.DataPump;

public class Driver {

    public static void main(String[] args) {
        DataPump dataPump = new DataPump();
        dataPump.runTask();
    }
}
