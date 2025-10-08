from dw import DW
import extract
import transform
import load


if __name__ == '__main__':
    dw = DW(create=True)

    # TODO: Write the control flow
    load.XXX(dw,
        transform.XXX(
            extract.XXX()
        )
    )

    dw.close()
