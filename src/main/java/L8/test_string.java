package L8;

public class test_string {
    public static void main(String args[]){
        String [] a = "Hello kitte 7".split(" ");
        String temp = "";
        for(int i=0;i<a.length-1;i++){
            temp+=(a[i]);
            temp+=(" ");
        }
        System.out.println(temp);
        System.out.println(a[a.length-1]);
    }
}
