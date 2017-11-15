/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

/**
 *
 * @author vahan
 */
public class barlusUser implements Serializable {

    private UUID id;
    private Double balance = 0.1;
    private boolean active;

    public barlusUser(ArrayList<KeyValue> row) {
        this.id = UUID.fromString(new String(row.get(0).key()));
        for (KeyValue property : row) {
            if (Arrays.equals(property.qualifier(), "active".getBytes())) {
                if (property.value().length == 1) {
                    this.active = property.value()[0] != (byte) 0;
                }
                if (property.value().length == 4) {
                    this.active = Bytes.getInt(property.value()) != 0;
                }
            }
            if (Arrays.equals(property.qualifier(), "balance".getBytes())) {
                this.balance = ByteBuffer.wrap(property.value()).getDouble();//Bytes.getLong(property.value());
            }

        }

    }

    /**
     * @return the id
     */
    public UUID getId() {
        return id;
    }

    /**
     * @return the balance
     */
    public Double getBalance() {        
        return balance;
    }

    /**
     * @param balance the balance to set
     */
    public void setBalance(Double balance) {
        this.balance = balance;
    }

    /**
     * @return the active
     */
    public boolean isActive() {
        return active;
    }
}
